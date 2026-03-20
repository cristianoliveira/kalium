/*
 * Wire
 * Copyright (C) 2024 Wire Swiss GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see http://www.gnu.org/licenses/.
 */

package com.wire.kalium.logic.feature.message

import com.wire.kalium.common.functional.flatMap
import com.wire.kalium.common.functional.fold
import com.wire.kalium.common.functional.getOrNull
import com.wire.kalium.common.functional.onFailure
import com.wire.kalium.common.logger.kaliumLogger
import com.wire.kalium.cryptography.utils.AES256Key
import com.wire.kalium.cryptography.utils.generateRandomAES256Key
import com.wire.kalium.logic.data.asset.AssetRepository
import com.wire.kalium.logic.data.id.ConversationId
import com.wire.kalium.logic.data.id.CurrentClientIdProvider
import com.wire.kalium.logic.data.id.QualifiedID
import com.wire.kalium.logic.data.id.toApi
import com.wire.kalium.logic.data.message.Message
import com.wire.kalium.logic.data.message.MessageContent
import com.wire.kalium.logic.data.message.PersistMessageUseCase
import com.wire.kalium.logic.data.message.linkpreview.MessageLinkPreview
import com.wire.kalium.logic.data.message.mention.MessageMention
import com.wire.kalium.logic.data.properties.UserPropertyRepository
import com.wire.kalium.logic.data.sync.SlowSyncRepository
import com.wire.kalium.logic.data.sync.SlowSyncStatus
import com.wire.kalium.logic.feature.selfDeletingMessages.ObserveSelfDeletionTimerSettingsForConversationUseCase
import com.wire.kalium.messaging.sending.MessageSender
import com.wire.kalium.util.KaliumDispatcher
import com.wire.kalium.util.KaliumDispatcherImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.datetime.Clock
import kotlin.time.Duration
import kotlin.time.TimeSource
import kotlin.uuid.Uuid

/**
 * @sample samples.logic.MessageUseCases.sendingBasicTextMessage
 * @sample samples.logic.MessageUseCases.sendingTextMessageWithMentions
 */
// todo(interface). extract interface for use case
@Suppress("LongParameterList")
public class SendTextMessageUseCase internal constructor(
    private val persistMessage: PersistMessageUseCase,
    private val selfUserId: QualifiedID,
    private val provideClientId: CurrentClientIdProvider,
    private val assetDataSource: AssetRepository,
    private val slowSyncRepository: SlowSyncRepository,
    private val messageSender: MessageSender,
    private val messageSendFailureHandler: MessageSendFailureHandler,
    private val userPropertyRepository: UserPropertyRepository,
    private val selfDeleteTimer: ObserveSelfDeletionTimerSettingsForConversationUseCase,
    private val dispatchers: KaliumDispatcher = KaliumDispatcherImpl,
    private val scope: CoroutineScope
) {

    public suspend operator fun invoke(
        conversationId: ConversationId,
        text: String,
        linkPreviews: List<MessageLinkPreview> = emptyList(),
        mentions: List<MessageMention> = emptyList(),
        quotedMessageId: String? = null
    ): MessageOperationResult = scope.async(dispatchers.io) {
        val sendTimer = TimeSource.Monotonic.markNow()
        kaliumLogger.i(
            "[tmp-send-trace] step=send_text_use_case_start conversationId=${conversationId.toLogString()} selfUserId=${selfUserId.toLogString()}"
        )
        val slowSyncTimer = TimeSource.Monotonic.markNow()
        slowSyncRepository.slowSyncStatus.first {
            it is SlowSyncStatus.Complete
        }
        kaliumLogger.i(
            "[tmp-send-trace] step=slow_sync_complete conversationId=${conversationId.toLogString()} elapsedMs=${slowSyncTimer.elapsedNow().inWholeMilliseconds}"
        )

        val generatedMessageUuid = Uuid.random().toString()
        kaliumLogger.i(
            "[tmp-send-trace] step=message_uuid_generated conversationId=${conversationId.toLogString()} messageId=$generatedMessageUuid"
        )
        val expectsReadConfirmation = userPropertyRepository.getReadReceiptsStatus()
        val messageTimer: Duration? = selfDeleteTimer(conversationId, true)
            .first()
            .duration

        val previews = uploadLinkPreviewImages(linkPreviews, conversationId)

        provideClientId().flatMap { clientId ->
            val message = Message.Regular(
                id = generatedMessageUuid,
                content = MessageContent.Text(
                    value = text,
                    linkPreviews = previews,
                    mentions = mentions,
                    quotedMessageReference = quotedMessageId?.let { quotedMessageId ->
                        MessageContent.QuoteReference(
                            quotedMessageId = quotedMessageId,
                            quotedMessageSha256 = null,
                            isVerified = true
                        )
                    }
                ),
                expectsReadConfirmation = expectsReadConfirmation,
                conversationId = conversationId,
                date = Clock.System.now(),
                senderUserId = selfUserId,
                senderClientId = clientId,
                status = Message.Status.Pending,
                editStatus = Message.EditStatus.NotEdited,
                expirationData = messageTimer?.let { Message.ExpirationData(it) },
                isSelfMessage = true
            )
            val persistTimer = TimeSource.Monotonic.markNow()
            kaliumLogger.i(
                "[tmp-send-trace] step=persist_message_start conversationId=${conversationId.toLogString()} messageId=${message.id}"
            )
            persistMessage(message).flatMap {
                kaliumLogger.i(
                    "[tmp-send-trace] step=persist_message_end conversationId=${conversationId.toLogString()} messageId=${message.id} elapsedMs=${persistTimer.elapsedNow().inWholeMilliseconds}"
                )
                val sendTimerInternal = TimeSource.Monotonic.markNow()
                kaliumLogger.i(
                    "[tmp-send-trace] step=message_sender_send_start conversationId=${conversationId.toLogString()} messageId=${message.id}"
                )
                messageSender.sendMessage(message).also { result ->
                    val state = when (result) {
                        is com.wire.kalium.common.functional.Either.Left -> "failure"
                        is com.wire.kalium.common.functional.Either.Right -> "success"
                    }
                    kaliumLogger.i(
                        "[tmp-send-trace] step=message_sender_send_end conversationId=${conversationId.toLogString()} messageId=${message.id} state=$state elapsedMs=${sendTimerInternal.elapsedNow().inWholeMilliseconds}"
                    )
                }
            }
        }.onFailure {
            kaliumLogger.e(
                "[tmp-send-trace] step=send_text_use_case_failure conversationId=${conversationId.toLogString()} messageId=$generatedMessageUuid failureType=${it::class.simpleName ?: "UnknownFailure"} elapsedMs=${sendTimer.elapsedNow().inWholeMilliseconds}"
            )
            messageSendFailureHandler.handleFailureAndUpdateMessageStatus(
                failure = it,
                conversationId = conversationId,
                messageId = generatedMessageUuid,
                messageType = TYPE
            )
        }.fold(
            {
                kaliumLogger.i(
                    "[tmp-send-trace] step=send_text_use_case_end conversationId=${conversationId.toLogString()} messageId=$generatedMessageUuid state=failure elapsedMs=${sendTimer.elapsedNow().inWholeMilliseconds}"
                )
                MessageOperationResult.Failure(it)
            },
            {
                kaliumLogger.i(
                    "[tmp-send-trace] step=send_text_use_case_end conversationId=${conversationId.toLogString()} messageId=$generatedMessageUuid state=success elapsedMs=${sendTimer.elapsedNow().inWholeMilliseconds}"
                )
                MessageOperationResult.Success
            }
        )
    }.await()

    internal companion object {
        internal const val TYPE = "Text"
    }

    private suspend fun uploadLinkPreviewImages(
        linkPreviews: List<MessageLinkPreview>,
        conversationId: ConversationId
    ): List<MessageLinkPreview> {
        return linkPreviews.map { linkPreview ->
            val imageCopy = linkPreview.image?.let {
                // Generate the otr asymmetric key that will be used to encrypt the data
                it.otrKey = generateRandomAES256Key().data
                // The assetDataSource will encrypt the data with the provided otrKey and upload it if successful
                it.assetDataPath?.let { assetDataPath ->
                    assetDataSource.uploadAndPersistPrivateAsset(
                        mimeType = it.mimeType,
                        assetDataPath = assetDataPath,
                        otrKey = AES256Key(it.otrKey),
                        extension = null,
                        conversationId = conversationId.toApi(),
                        filename = "link-preview-${linkPreview.url}",
                        filetype = it.mimeType
                    ).onFailure { failure ->
                        // on upload failure we still want link previews being included without image
                        kaliumLogger.e("Upload of link preview asset failed: $failure")
                    }.getOrNull()?.let { (assetId, sha256Key) ->
                        it.assetToken = assetId.assetToken ?: ""
                        it.assetKey = assetId.key
                        it.assetDomain = assetId.domain
                        it.sha256Key = sha256Key.data
                        it
                    }
                }
            }
            linkPreview.copy(image = imageCopy)
        }
    }
}
