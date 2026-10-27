package core

// These are execution bounds, not a requirement to fill a migration group.
// The legacy finite fixture defaults to three groups. Generation-aware runtime
// history is not bounded by this fixture count. Each command remains bounded;
// remigration remains unsupported and histories/fences are retained.
const MinMigrationGroupMembers = 1
const MaxMigrationGroupMembers = 32
const MaxSerialMigrationGroups = 3
