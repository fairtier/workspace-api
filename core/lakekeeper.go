package core

// Warehouse represents an Iceberg warehouse in Lakekeeper.
type Warehouse struct {
	ID   string
	Name string
}

// LakekeeperUser represents a user registered in Lakekeeper.
type LakekeeperUser struct {
	ID   string
	Name string
}

// WarehouseAssignment represents a single user-to-relation assignment on a warehouse.
type WarehouseAssignment struct {
	UserID   string // Lakekeeper principal ID (e.g. "oidc~admin/lk-acme-writer")
	Relation string // e.g. "describe", "select", "create", "modify", "ownership"
}

// CasdoorApp represents a per-user Casdoor application (service account).
type CasdoorApp struct {
	Name         string
	ClientID     string
	ClientSecret string
}
