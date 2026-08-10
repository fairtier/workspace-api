// Package objstore is a minimal S3-compatible object client used by the
// file-drop feature to write into a customer's own bucket (managed R2 or
// BYOS) with the per-customer credentials Terraform resolved into
// ActualConfig.EffectiveS3.
//
// Deliberately tiny, same philosophy as kube.R2Cleaner's S3 path: two
// operations (PutObject, DeleteObject), path-style addressing, hand-rolled
// SigV4. PUT signs with UNSIGNED-PAYLOAD so the request body streams straight
// from the caller without buffering or double-reading — accepted by R2,
// MinIO, and AWS-over-HTTPS alike.
package objstore

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/telemetry"
)

// Client implements workspace.ObjectStore against any S3-compatible endpoint.
type Client struct {
	client *http.Client
	now    func() time.Time // seam for signing tests
}

// New creates a Client. The generous timeout bounds a full streamed upload,
// not a single round trip.
func New() *Client {
	return &Client{
		// Instrumented: an upload that stalls is the customer's whole
		// experience of the feature, and this span is the only place the
		// bucket round trip is visible.
		client: telemetry.InstrumentHTTPClient(&http.Client{Timeout: 15 * time.Minute}),
		now:    time.Now,
	}
}

// Put streams body (exactly size bytes) to key in cfg's bucket.
func (c *Client) Put(ctx context.Context, cfg core.S3Config, key string, size int64, body io.Reader) error {
	req, encodedPath, err := c.newRequest(ctx, cfg, http.MethodPut, key)
	if err != nil {
		return err
	}
	req.Body = io.NopCloser(body)
	req.ContentLength = size

	signRequest(req, encodedPath, cfg, unsignedPayload, c.now().UTC())
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("s3 put %q: HTTP %d: %s", key, resp.StatusCode, string(msg))
	}
	return nil
}

// Delete removes key from cfg's bucket. A 404 is success: the goal (object
// gone) is already met.
func (c *Client) Delete(ctx context.Context, cfg core.S3Config, key string) error {
	req, encodedPath, err := c.newRequest(ctx, cfg, http.MethodDelete, key)
	if err != nil {
		return err
	}
	signRequest(req, encodedPath, cfg, emptyPayloadSHA256, c.now().UTC())
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("s3 delete %q: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("s3 delete %q: HTTP %d: %s", key, resp.StatusCode, string(msg))
	}
	return nil
}

// Head reports whether key exists in cfg's bucket. A 2xx means present, a 404
// means absent; any other status (or a transport error) is returned as an
// error so callers can distinguish "definitely gone" from "could not tell".
func (c *Client) Head(ctx context.Context, cfg core.S3Config, key string) (bool, error) {
	req, encodedPath, err := c.newRequest(ctx, cfg, http.MethodHead, key)
	if err != nil {
		return false, err
	}
	signRequest(req, encodedPath, cfg, emptyPayloadSHA256, c.now().UTC())
	resp, err := c.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("s3 head %q: %w", key, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return false, nil
	case resp.StatusCode < 300:
		return true, nil
	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("s3 head %q: HTTP %d: %s", key, resp.StatusCode, string(msg))
	}
}

// newRequest builds the unsigned path-style request for an object operation
// and returns the canonically-encoded path used for signing.
func (c *Client) newRequest(ctx context.Context, cfg core.S3Config, method, key string) (*http.Request, string, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, "", fmt.Errorf("s3 config missing endpoint or bucket")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, "", fmt.Errorf("s3 config missing credentials")
	}
	encodedPath := "/" + cfg.Bucket + "/" + awsURIEncode(key, false)
	url := strings.TrimSuffix(cfg.Endpoint, "/") + encodedPath
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, "", err
	}
	return req, encodedPath, nil
}

const (
	emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload    = "UNSIGNED-PAYLOAD"
)

// signRequest adds AWS SigV4 headers for a request with no query string.
// payloadHash is either the empty-body constant or UNSIGNED-PAYLOAD for
// streamed bodies. Region defaults to "auto" (R2's convention) when unset.
func signRequest(req *http.Request, encodedPath string, cfg core.S3Config, payloadHash string, now time.Time) {
	region := cfg.Region
	if region == "" {
		region = "auto"
	}
	const service = "s3"

	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	const signedHeaders = "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		req.Method,
		encodedPath,
		"", // query
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{dateStamp, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hexSHA256(canonicalRequest),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+cfg.SecretAccessKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		cfg.AccessKeyID, scope, signedHeaders, signature))
}

func hexSHA256(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// awsURIEncode implements the AWS SigV4 URI-encoding rules: every byte is
// percent-encoded except unreserved characters, and '/' stays literal when
// encodeSlash is false (object keys keep their path shape).
func awsURIEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
