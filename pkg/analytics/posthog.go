package analytics

import (
	"github.com/posthog/posthog-go"
	"github.com/rs/zerolog/log"
)

type AnalyticsClient interface {
	Identify(distinctId string, properties map[string]interface{})
	Capture(distinctId string, event string, properties map[string]interface{})
	Close()
}

type postHogClient struct {
	client posthog.Client
}

func NewPostHogClient(apiKey string, endpoint string) (AnalyticsClient, error) {
	client, err := posthog.NewWithConfig(apiKey, posthog.Config{
		Endpoint: endpoint,
	})
	if err != nil {
		return nil, err
	}
	return &postHogClient{client: client}, nil
}

func (p *postHogClient) Identify(distinctId string, properties map[string]interface{}) {
	err := p.client.Enqueue(posthog.Identify{
		DistinctId: distinctId,
		Properties: posthog.Properties(properties),
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to enqueue identify event")
	}
}

func (p *postHogClient) Capture(distinctId string, event string, properties map[string]interface{}) {
	err := p.client.Enqueue(posthog.Capture{
		DistinctId: distinctId,
		Event:      event,
		Properties: posthog.Properties(properties),
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to enqueue capture event")
	}
}

func (p *postHogClient) Close() {
	p.client.Close()
}
