package workspace

import (
	"errors"
	"testing"
)

func TestValidateS3Endpoint(t *testing.T) {
	valid := []string{
		"", // Lakekeeper falls back to the region's real S3
		"https://s3.eu-central-1.amazonaws.com",
		"https://accountid.r2.cloudflarestorage.com",
		"http://minio.example.com:9000",
		"https://minio.example.com/",
		"https://203.0.113.10:9000",
	}
	for _, e := range valid {
		if err := validateS3Endpoint(e); err != nil {
			t.Errorf("validateS3Endpoint(%q) = %v, want nil", e, err)
		}
	}

	invalid := []string{
		"ftp://example.com",
		"s3.example.com", // no scheme
		"https://",
		"https://user:pass@example.com",
		"https://example.com/some/path",
		"https://example.com?query=1",
		"https://169.254.169.254",        // cloud metadata
		"http://169.254.169.254/latest/", // cloud metadata with path
		"https://127.0.0.1:9000",
		"https://[::1]:9000",
		"https://10.0.0.5",
		"https://172.16.3.4",
		"https://192.168.1.1:9000",
		"https://[fd00::1]",
		"https://0.0.0.0",
		"https://minio",     // single-label internal hostname
		"https://localhost", // single label, and loopback by convention
		"https://minio.local",
		"https://vault.internal",
		"https://svc.ns.svc.cluster.local",
		"https://box.localhost",
	}
	for _, e := range invalid {
		if err := validateS3Endpoint(e); !errors.Is(err, ErrInvalidS3Endpoint) {
			t.Errorf("validateS3Endpoint(%q) = %v, want ErrInvalidS3Endpoint", e, err)
		}
	}
}
