package com.samistax.astra.functions;

import com.samistax.astra.entity.PulsarChatMessage;
import com.theokanning.openai.moderation.ModerationRequest;
import com.theokanning.openai.moderation.ModerationResult;
import com.theokanning.openai.service.OpenAiService;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.time.Duration;
import java.util.Map;

public class ChatMessageModerator implements Function<PulsarChatMessage, PulsarChatMessage> {

    private static String OPENAI_API_KEY = "YOUR_API_KEY";
    private static String MODERATED_TOPIC = "persistent://aidemo-stream/default/moderated-messages";
    private static String FLAGGED_TOPIC = "persistent://aidemo-stream/default/flagged-messages";;

    private static OpenAiService openAIService;

    @Override
    public void initialize(Context context) {

        // Access the custom properties from the Context object
        Map<String, Object> properties = context.getUserConfigMap();
        try {
            properties.values().stream().forEach(v -> System.out.println("Val: " + v.toString()));
            properties.keySet().stream().forEach(k -> System.out.println("Key: " + k.toString()));

            OPENAI_API_KEY = ""+context.getUserConfigValueOrDefault("apikey", "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");
            MODERATED_TOPIC = (String) properties.get("moderation_topic");
            FLAGGED_TOPIC = (String) properties.get("flagged_topic");

            System.out.println("apikey: " + OPENAI_API_KEY);
            System.out.println("moderation_topic: " + MODERATED_TOPIC);
            System.out.println("flagged_topic: " + FLAGGED_TOPIC);

            openAIService = new OpenAiService(OPENAI_API_KEY, Duration.ofSeconds(20L));
        } catch (Exception ex) {
            System.out.println("Init exception: " + ex);
        }
    }
    @Override
    public PulsarChatMessage process(PulsarChatMessage input, Context context) {

        // Publish the message to appropriate topics based on moderation result
        if ( openAIService != null )  {

            long startTime = System.currentTimeMillis();

            // Use OpenAI Moderation API to evaluate the chat message
            ModerationRequest req = ModerationRequest.builder()
                    .model("text-moderation-latest")
                    .input(input.getText()).build();

            ModerationResult result = openAIService.createModeration(req);
            System.out.println("Msg in topic ("+input.getTopicId()+") moderated in: " + (System.currentTimeMillis() - startTime) +" (ms)");
/*
            if ( result.getResults().stream().filter(r -> r.isFlagged()).count() > 0  ) {
                System.out.println("FLAGGED MESSAGE: " + input.getText());

                // Based on moderation results, one could send flagged messages to another topic
                Optional<Moderation> res = result.getResults().stream().filter(m -> m.isFlagged()).findFirst();
                try {
                    if ( res.isEmpty() ) {
                        context.newOutputMessage(MODERATED_TOPIC, Schema.JSON(PulsarChatMessage.class)).value(input).sendAsync();
                    } else {
                        if ( FLAGGED_TOPIC != null ) {
                            context.newOutputMessage(FLAGGED_TOPIC, Schema.JSON(PulsarChatMessage.class)).value(input).sendAsync();
                        }
                        // Do not publish the flagged message to output topic
                        return  null;
                    }
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }

            }
 */
            // Enrich chat message with  moderation results, that are pushed to output topic.
            input.setModerationResult(result);
        }
        return input;
    }
}
