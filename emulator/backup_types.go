package emulator

import (
	"strings"
	"time"
)

// backupNamespace is the state namespace for AWS Backup resources.
const backupNamespace = "backup"

// BackupVault represents an AWS Backup vault.
type BackupVault struct {
	// BackupVaultName is the name of the backup vault.
	BackupVaultName string `json:"BackupVaultName"`
	// BackupVaultArn is the ARN of the backup vault.
	BackupVaultArn string `json:"BackupVaultArn"`
	// EncryptionKeyArn is the ARN of the KMS key used for encryption.
	EncryptionKeyArn string `json:"EncryptionKeyArn,omitempty"`
	// CreationDate is when the vault was created.
	CreationDate time.Time `json:"CreationDate"`
	// NumberOfRecoveryPoints is the number of recovery points in the vault.
	NumberOfRecoveryPoints int64 `json:"NumberOfRecoveryPoints"`
	// AccountID is the AWS account that owns this vault.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the vault exists.
	Region string `json:"Region"`
}

// BackupPlan represents an AWS Backup plan.
type BackupPlan struct {
	// BackupPlanID is the unique identifier for the backup plan.
	BackupPlanID string `json:"BackupPlanId"`
	// BackupPlanArn is the ARN of the backup plan.
	BackupPlanArn string `json:"BackupPlanArn"`
	// BackupPlanName is the display name of the backup plan.
	BackupPlanName string `json:"BackupPlanName"`
	// Rules contains the backup rules for this plan.
	Rules []map[string]interface{} `json:"Rules,omitempty"`
	// VersionID is the unique, randomly generated, Unicode, UTF-8 encoded version ID.
	VersionID string `json:"VersionId"`
	// CreationDate is when the plan was created.
	CreationDate time.Time `json:"CreationDate"`
	// LastExecutionDate is the last time the plan was executed.
	LastExecutionDate *time.Time `json:"LastExecutionDate,omitempty"`
	// AccountID is the AWS account that owns this plan.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the plan exists.
	Region string `json:"Region"`
}

// BackupSelection represents an AWS Backup selection (resources assigned to a plan).
type BackupSelection struct {
	// SelectionID is the unique identifier for the backup selection.
	SelectionID string `json:"SelectionId"`
	// SelectionName is the display name of the backup selection.
	SelectionName string `json:"SelectionName"`
	// BackupPlanID is the ID of the backup plan this selection belongs to.
	BackupPlanID string `json:"BackupPlanId"`
	// IamRoleArn is the ARN of the IAM role for the backup selection.
	IamRoleArn string `json:"IamRoleArn,omitempty"`
	// Resources is the list of ARNs for resources to back up.
	Resources []string `json:"Resources,omitempty"`
	// CreationDate is when the selection was created.
	CreationDate time.Time `json:"CreationDate"`
	// AccountID is the AWS account that owns this selection.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the selection exists.
	Region string `json:"Region"`
}

// parseBackupOperation maps an HTTP method and URL path to an AWS Backup
// operation name and optional resource identifiers. It follows the same
// pattern as parseEFSOperation in efs_types.go.
//
// The path is matched by whole segments, each literal at its published position (#1205's sweep).
// It used to test prefixes and search for "/selections" as a substring, so /backup-vaultsX routed as
// a vault operation, /backup/plansX as a plan operation, and /backup/plans/a/b/selections read the
// plan ID as "a/b". A single trailing slash is accepted, because several of the published URIs carry
// one (ListBackupVaults' /backup-vaults/, ListBackupPlans' /backup/plans/, GetBackupPlan's
// /backup/plans/{backupPlanId}/). Before, that slash was kept in GetBackupPlan's plan ID, so the
// published URI looked up a plan that could not exist.
//
// An empty identifier is still routed to its single-resource operation, as it was, so each
// handler's "… is required" refusal stays reachable. That is the #1009 reasoning: a refusal a
// caller can act on beats another operation's success.
func parseBackupOperation(method, path string) (op, vaultName, planID, selectionID string) {
	segs := strings.Split(strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/"), "/")

	// /backup-vaults[/{name}]
	if segs[0] == "backup-vaults" && len(segs) <= 2 {
		name := ""
		if len(segs) == 2 {
			name = segs[1]
		}
		switch method {
		case "PUT":
			return "CreateBackupVault", name, "", ""
		case "GET":
			if len(segs) == 1 {
				return "ListBackupVaults", "", "", ""
			}
			return "DescribeBackupVault", name, "", ""
		case "DELETE":
			return "DeleteBackupVault", name, "", ""
		}
		return "", "", "", ""
	}

	// /backup/plans[/{planId}[/selections[/{selectionId}]]]
	if len(segs) < 2 || segs[0] != "backup" || segs[1] != "plans" {
		return "", "", "", ""
	}
	switch len(segs) {
	case 2, 3:
		pid := ""
		if len(segs) == 3 {
			pid = segs[2]
		}
		switch method {
		case "POST":
			if len(segs) == 2 {
				return "CreateBackupPlan", "", "", ""
			}
			return "UpdateBackupPlan", "", pid, ""
		case "GET":
			if len(segs) == 2 {
				return "ListBackupPlans", "", "", ""
			}
			return "GetBackupPlan", "", pid, ""
		case "DELETE":
			return "DeleteBackupPlan", "", pid, ""
		}
	case 4, 5:
		if segs[3] != "selections" {
			return "", "", "", ""
		}
		sel := ""
		if len(segs) == 5 {
			sel = segs[4]
		}
		switch method {
		case "POST":
			if len(segs) == 4 {
				return "CreateBackupSelection", "", segs[2], ""
			}
		case "GET":
			return "GetBackupSelection", "", segs[2], sel
		case "DELETE":
			return "DeleteBackupSelection", "", segs[2], sel
		}
	}
	return "", "", "", ""
}

// State key helpers.

func backupVaultKey(acct, region, name string) string {
	return "vault:" + acct + "/" + region + "/" + name
}

func backupVaultNamesKey(acct, region string) string {
	return "vault_names:" + acct + "/" + region
}

func backupPlanKey(acct, region, planID string) string {
	return "plan:" + acct + "/" + region + "/" + planID
}

func backupPlanIDsKey(acct, region string) string {
	return "plan_ids:" + acct + "/" + region
}

func backupSelectionKey(acct, region, planID, selectionID string) string {
	return "selection:" + acct + "/" + region + "/" + planID + "/" + selectionID
}

func backupSelectionIDsKey(acct, region, planID string) string {
	return "selection_ids:" + acct + "/" + region + "/" + planID
}
