package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// Priority is the value sent in the X-Priority header to the gateway.
type Priority int

const (
	PriorityTransactions Priority = 100
	PriorityRegions      Priority = 80
	PriorityDamage       Priority = 70
	PriorityTradeOffers  Priority = 60
	PriorityUser         Priority = 50
	PriorityCompanies    Priority = 30
	PriorityCountries    Priority = 10
	PriorityUserRefresh  Priority = -10
)

// Client is a tRPC-over-HTTP client for the upstream gateway.
type Client struct {
	addr string
	http *http.Client
}

func NewClient(addr string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{addr: addr, http: httpClient}
}

type trpcResponse struct {
	Result struct {
		Data json.RawMessage `json:"data"`
	} `json:"result"`
}

func (c *Client) do(ctx context.Context, method string, body map[string]any, prio Priority) (json.RawMessage, error) {
	if body == nil {
		body = map[string]any{}
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/"+method, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Priority", strconv.Itoa(int(prio)))

	resp, err := c.http.Do(req)
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
