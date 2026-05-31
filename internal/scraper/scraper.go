package scraper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
)

// http://10.110.16.2:8080
var gateway_addr string

func init() {
	gateway_addr = os.Getenv("GATEWAY_ADDR")

	if gateway_addr == "" {
		slog.Error("Gateway address is not set!")
		os.Exit(1)
	}
}

type trpcResponse struct {
	Result struct {
		Data json.RawMessage `json:"data"`
	} `json:"result"`
}

func req(method string, body map[string]any, prio int) (json.RawMessage, error) {
	if body == nil {
		body = map[string]any{}
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}

	request, err := http.NewRequest(http.MethodPost, gateway_addr+"/"+method, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Priority", strconv.Itoa(prio))

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, data)
	}

	var trpc trpcResponse
	if err := json.Unmarshal(data, &trpc); err != nil {
		return nil, fmt.Errorf("unmarshal trpc response: %w", err)
	}

	return trpc.Result.Data, nil
}
