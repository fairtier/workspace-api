// The Google OAuth scope strings, in the shared kernel because three layers
// need the same literals and none of them may import the others: oauthgoogle
// (the adapter) builds the consent URL from them, workspace (the domain)
// checks a stored connection against them at save time, and server reports
// them to the Console. depguard forbids workspace from importing the adapter,
// so a copy per package would be a parity trap of exactly the kind this repo
// keeps writing down — a scope string that drifts silently authorizes nothing
// and fails hours later, on the box.
package core

const (
	// GoogleSheetsReadonlyScope reads spreadsheet values by ID. Sensitive, but
	// deliberately not a drive.* restricted scope.
	GoogleSheetsReadonlyScope = "https://www.googleapis.com/auth/spreadsheets.readonly"

	// GoogleDriveFileScope reaches individual Drive files — the ones the user
	// picks through Google's Picker, or that the app created — never the whole
	// Drive. It is what a duckdb/gdrive pipeline needs, and it is not a
	// restricted scope, so nobody faces Google's third-party security
	// assessment for it. See oauthgoogle.DriveFileScope for what was verified
	// against the real extension, and what it does not cover.
	GoogleDriveFileScope = "https://www.googleapis.com/auth/drive.file"
)
