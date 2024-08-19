package openlineage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	// "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/tidwall/pretty"
)

var (
	_ Transport = (*httpTransport)(nil)
)

type TransportType string

const (
	TransportHTTP    TransportType = "http"
	TransportConsole TransportType = "console"
	// TransportKafka = "kafka"
	// TransportFile = "file"
)

type Transport interface {
	Emit(ctx context.Context, event any) error
}

type httpTransport struct {
	httpClient *http.Client
	uri        string
	apiKey     string
}

// Emit implements transport.
func (h *httpTransport) Emit(ctx context.Context, event any) error {
	body, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		h.uri,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if h.apiKey != "" {
		bearer := fmt.Sprintf("Bearer %s", h.apiKey)
		req.Header.Add("Authorization", bearer)
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute POST request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server responded with status %v: %s", resp.StatusCode, body)
	}

	return nil
}

type consoleTransport struct {
	prettyPrint bool
}

func (ct *consoleTransport) Emit(ctx context.Context, event any) error {
	body, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if ct.prettyPrint {
		body = pretty.Pretty(body)
	}

	if _, err := fmt.Println(string(body)); err != nil {
		return fmt.Errorf("emit event to console: %w", err)
	}

	return nil
}
