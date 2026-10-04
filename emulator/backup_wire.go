package emulator

// The wire is a different thing from the state, and the type below exists to keep them
// apart.
//
// BackupVault (backup_types.go) is a persisted state record, and it was handed straight
// to the caller at both sites that answer a vault — describeBackupVault, and
// listBackupVaults through a []BackupVault. Two of its fields are substrate's own, and
// neither carries omitempty, so both responses answered AccountID and Region, which no
// Backup shape publishes (#756). The plan and selection handlers already build their
// responses member by member, so the vault was the one Backup record with no projection.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field
// and leaves the next one to be remembered rather than prevented, and it changes the
// format of every recorded run, because MemoryStateManager snapshots those bytes and a
// replay reads them back. For the same reason CreationDate stays a time.Time in the
// record and is converted on projection rather than being retyped in place.
//
// # Why every Backup date is EpochSeconds
//
// Backup speaks REST-JSON, and every date it publishes is a Unix timestamp rather than an
// RFC3339 string. The pages say so in as many words — API_DescribeBackupVault,
// API_BackupVaultListMember, API_CreateBackupVault, API_GetBackupPlan,
// API_BackupPlansListMember, API_CreateBackupPlan, API_GetBackupSelection and
// API_CreateBackupSelection each gloss CreationDate as "in Unix format and Coordinated
// Universal Time (UTC). The value of CreationDate is accurate to milliseconds. For
// example, the value 1516925490.087" — and every Response Syntax on those pages renders
// the member as `number`. A Go time.Time marshals to RFC3339, which is what all nine
// sites answered; EpochSeconds (epochseconds.go) marshals to exactly three decimals,
// which is the published precision (#1324).
//
// The conversion happens at the response site in every case, including in the maps the
// plan and selection handlers build. There is no second wire struct for them because a
// plan or selection response nests its record's members under BackupPlan or
// BackupSelection rather than answering them flat, so the map *is* the projection; what it
// was missing was the date's form, not a type.

// backupVaultOut is the vault element of the Backup vault responses: DescribeBackupVault,
// and — under the name BackupVaultList — ListBackupVaults.
//
// One type for both sites, because every member substrate answers is in API_DescribeBackupVault's
// seventeen *and* API_BackupVaultListMember's thirteen, and every member of both shapes is
// Required: No.
//
// # What it answers (#1199)
//
// Nine members: BackupVaultName, BackupVaultArn, EncryptionKeyArn (when the create named one),
// CreationDate, NumberOfRecoveryPoints, CreatorRequestId (when the create sent one), and three
// whose values follow from what substrate routes rather than from anything stored:
//
//   - VaultState is AVAILABLE. A vault is usable the moment CreateBackupVault returns, and neither
//     CREATING nor FAILED is ever observable. VaultState is the only published signal that a vault is
//     anything but AVAILABLE, so answering it lets a consumer's state check run.
//   - VaultType is BACKUP_VAULT. CreateBackupVault makes that type, and the operations that make the
//     other two (CreateLogicallyAirGappedBackupVault, CreateRestoreAccessBackupVault) are unrouted.
//   - Locked is false. PutBackupVaultLockConfiguration is unrouted, so no vault is ever locked, and
//     false is the truth rather than a zero value standing in for an unknown.
//
// # What it does not, and why
//
// The rest are absent because no vault ever has a value for them:
//
//   - LockDate, MinRetentionDays and MaxRetentionDays are the Vault Lock settings, which the page
//     says are unset when no lock is configured — and none can be.
//   - EncryptionKeyType is not answered. Substrate records the key ARN a create names, but whether
//     that key is customer-managed or AWS-owned depends on the key, which the record does not hold,
//     and a create that names none would report AWS's default backup key, which substrate does not
//     mint. Either value would be a guess.
//   - SourceBackupVaultArn belongs to a restore-access vault, and MpaApprovalTeamArn, MpaSessionArn
//     and LatestMpaApprovalTeamUpdate to multi-party approval; neither is routed.
//
// CreateBackupVault is deliberately *not* projected through this type: API_CreateBackupVault
// publishes three members and no more, so it keeps its own map and converts its date there.
type backupVaultOut struct {
	BackupVaultName        string       `json:"BackupVaultName"`
	BackupVaultArn         string       `json:"BackupVaultArn"`
	EncryptionKeyArn       string       `json:"EncryptionKeyArn,omitempty"`
	CreationDate           EpochSeconds `json:"CreationDate"`
	NumberOfRecoveryPoints int64        `json:"NumberOfRecoveryPoints"`
	CreatorRequestID       string       `json:"CreatorRequestId,omitempty"`
	VaultState             string       `json:"VaultState"`
	VaultType              string       `json:"VaultType"`
	Locked                 bool         `json:"Locked"`
}

// backupVaultStateAvailable is the one VaultState a substrate vault is ever in.
const backupVaultStateAvailable = "AVAILABLE"

// backupVaultToWire projects a persisted vault onto the published shape.
func backupVaultToWire(vault BackupVault) backupVaultOut {
	return backupVaultOut{
		BackupVaultName:        vault.BackupVaultName,
		BackupVaultArn:         vault.BackupVaultArn,
		EncryptionKeyArn:       vault.EncryptionKeyArn,
		CreationDate:           EpochSeconds(vault.CreationDate),
		NumberOfRecoveryPoints: vault.NumberOfRecoveryPoints,
		CreatorRequestID:       vault.CreatorRequestID,
		VaultState:             backupVaultStateAvailable,
		VaultType:              backupVaultTypeStandard,
		Locked:                 false,
	}
}

// backupVaultsToWire projects a slice of persisted vaults, for ListBackupVaults.
//
// A non-nil empty slice, so an account with no vault answers `"BackupVaultList":[]` rather
// than `null` — which is the shape API_ListBackupVaults publishes for the member and what
// the site answered before the projection existed.
func backupVaultsToWire(vaults []BackupVault) []backupVaultOut {
	out := make([]backupVaultOut, 0, len(vaults))
	for _, vault := range vaults {
		out = append(out, backupVaultToWire(vault))
	}
	return out
}
