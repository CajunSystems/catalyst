package com.cajunsystems.catalyst.model;

import com.cajunsystems.catalyst.engine.Hashing;
import com.cajunsystems.catalyst.events.EventJson;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Native tool calling on {@link Message} is additive. A message without the new fields must record
 * and hash exactly as before they existed, or every recorded model call would read as a divergence
 * on replay.
 */
class MessageToolCallTest {

    private static final CompletionRequest PLAIN = CompletionRequest.of(Prompt.builder()
            .system("You triage tickets.").user("printer offline").build());

    /** The request hash as computed before tool-call fields existed, written out independently. */
    private static String legacyHash(CompletionRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("msgs:").append(request.prompt().messages().size()).append('\n');
        for (Message m : request.prompt().messages()) {
            field(sb, m.role().name());
            field(sb, m.content());
        }
        sb.append("tools:").append(request.tools().size()).append('\n');
        for (ToolSpec t : request.tools()) {
            field(sb, t.name());
            field(sb, t.inputSchema() == null ? "" : t.inputSchema());
        }
        return Hashing.sha256(sb.toString());
    }

    private static void field(StringBuilder sb, String value) {
        sb.append(value.getBytes(StandardCharsets.UTF_8).length).append(':').append(value).append('\n');
    }

    @Test
    void aMessageWithoutToolCallsHashesExactlyAsBefore() {
        assertThat(Hashing.canonicalRequestHash(PLAIN)).isEqualTo(legacyHash(PLAIN));
    }

    @Test
    void aMessageWithoutToolCallsRecordsExactlyAsBefore() {
        assertThat(EventJson.shared().valueToTree(new Message(Role.USER, "hi")).toString())
                .isEqualTo("{\"role\":\"USER\",\"content\":\"hi\"}");
    }

    @Test
    void toolCallsAndToolResultsAreRecordedAndHashed() throws Exception {
        ToolCall call = new ToolCall("call-1", "lookup", "{\"id\":\"T-1\"}");
        CompletionRequest withCalls = CompletionRequest.of(new Prompt(List.of(new Message(Role.USER, "go"),
                Message.assistantToolCalls("", List.of(call)), Message.toolResult(call, "found"))));
        CompletionRequest sameTextNoCalls = CompletionRequest.of(new Prompt(List.of(new Message(Role.USER, "go"),
                new Message(Role.ASSISTANT, ""), new Message(Role.TOOL, "found"))));

        assertThat(Hashing.canonicalRequestHash(withCalls)).isNotEqualTo(Hashing.canonicalRequestHash(sameTextNoCalls));

        String json = EventJson.shared().valueToTree(withCalls.prompt()).toString();
        assertThat(json).contains("\"toolCalls\":[{\"id\":\"call-1\",\"name\":\"lookup\"")
                .contains("\"toolCallId\":\"call-1\"").contains("\"toolName\":\"lookup\"");
        Prompt back = EventJson.shared().treeToValue(EventJson.shared().readTree(json), Prompt.class);
        assertThat(back).isEqualTo(withCalls.prompt());
    }

    @Test
    void onlyTheRightRolesCarryToolCallFields() {
        ToolCall call = new ToolCall("c", "t", "{}");
        assertThatThrownBy(() -> new Message(Role.USER, "x", List.of(call), null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Message(Role.USER, "x", List.of(), "c", "t")).isInstanceOf(IllegalArgumentException.class);
    }
}
