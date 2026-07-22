package oauthpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
	xproxy "golang.org/x/net/proxy"
)

type poolClient struct {
	client *http.Client
	err    error
}

type transportManager struct {
	mu      sync.RWMutex
	config  config.ProxyConfig
	clients map[storage.OAuthPool]poolClient
}

func (m *transportManager) init() {
	m.clients = make(map[storage.OAuthPool]poolClient)
	_ = m.update(config.ProxyConfig{})
}

func (s *Service) UpdateProxyConfig(value config.ProxyConfig) error {
	return s.transport.update(value)
}

func (m *transportManager) update(value config.ProxyConfig) error {
	clients := make(map[storage.OAuthPool]poolClient, 2)
	var updateErrors []error
	for _, pool := range []storage.OAuthPool{storage.OAuthPoolChatGPT, storage.OAuthPoolGrok} {
		proxyURL, err := proxyURLForPool(value, pool)
		if err != nil {
			clients[pool] = poolClient{err: fmt.Errorf("OAuth pool proxy configuration: %w", err)}
			updateErrors = append(updateErrors, err)
			continue
		}
		client, err := buildHTTPClient(proxyURL)
		if err != nil {
			clients[pool] = poolClient{err: err}
			updateErrors = append(updateErrors, err)
			continue
		}
		clients[pool] = poolClient{client: client}
	}
	m.mu.Lock()
	old := m.clients
	m.config = value
	m.clients = clients
	m.mu.Unlock()
	for _, value := range old {
		if value.client != nil {
			value.client.CloseIdleConnections()
		}
	}
	return errors.Join(updateErrors...)
}

func proxyURLForPool(value config.ProxyConfig, pool storage.OAuthPool) (string, error) {
	targets := []string{config.ProxyTargetGrokPool}
	if pool == storage.OAuthPoolChatGPT {
		targets = []string{config.ProxyTargetChatGPTPool}
	}
	if !value.AppliesTo(targets...) {
		return "", nil
	}
	// Fail closed: an enabled, selected proxy must be valid. It is never
	// silently converted into a direct connection.
	return value.URL()
}

func buildHTTPClient(proxyURL string) (*http.Client, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = dialer.DialContext
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 16
	transport.MaxConnsPerHost = 32
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.ExpectContinueTimeout = time.Second

	if strings.TrimSpace(proxyURL) != "" {
		parsed, err := url.Parse(proxyURL)
		if err != nil || parsed.Hostname() == "" {
			return nil, errors.New("proxy URL is invalid")
		}
		switch strings.ToLower(parsed.Scheme) {
		case "http", "https":
			transport.Proxy = http.ProxyURL(parsed)
		case "socks5", "socks5h":
			var auth *xproxy.Auth
			if parsed.User != nil {
				password, _ := parsed.User.Password()
				auth = &xproxy.Auth{User: parsed.User.Username(), Password: password}
			}
			socksDialer, err := xproxy.SOCKS5("tcp", parsed.Host, auth, dialer)
			if err != nil {
				return nil, fmt.Errorf("create SOCKS5 proxy: %w", err)
			}
			if contextual, ok := socksDialer.(xproxy.ContextDialer); ok {
				transport.DialContext = contextual.DialContext
			} else {
				transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					type result struct {
						connection net.Conn
						err        error
					}
					completed := make(chan result, 1)
					go func() {
						connection, dialErr := socksDialer.Dial(network, address)
						completed <- result{connection: connection, err: dialErr}
					}()
					select {
					case value := <-completed:
						return value.connection, value.err
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			}
		default:
			return nil, fmt.Errorf("unsupported proxy protocol %q", parsed.Scheme)
		}
	}
	return &http.Client{Transport: transport}, nil
}

func (m *transportManager) client(pool storage.OAuthPool) (*http.Client, error) {
	m.mu.RLock()
	value, exists := m.clients[pool]
	m.mu.RUnlock()
	if !exists {
		return nil, errors.New("OAuth pool HTTP client is not configured")
	}
	if value.err != nil {
		return nil, value.err
	}
	if value.client == nil {
		return nil, errors.New("OAuth pool HTTP client is unavailable")
	}
	return value.client, nil
}

func (s *Service) Do(ctx context.Context, pool storage.OAuthPool, resolved ResolvedRequest) (*http.Response, error) {
	client, err := s.transport.client(pool)
	if err != nil {
		return nil, err
	}
	request, err := resolved.HTTPRequest(ctx)
	if err != nil {
		return nil, err
	}
	return client.Do(request)
}
