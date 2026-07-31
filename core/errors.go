// Sentinel errors shared by both planes' adapters (e.g. the Casdoor app
// manager, which the workspace plane drives for service accounts and the
// control plane for identity sync).
package core

import "errors"

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrUserAlreadyExists = errors.New("user already exists")
)
