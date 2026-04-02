package substrate

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
func parseBackupOperation(method, path string) (op, vaultName, planID, selectionID string) {
	// /backup-vaults[/{name}]
	if strings.HasPrefix(path, "/backup-vaults") {
		rest := strings.TrimPrefix(path, "/backup-vaults")
		rest = strings.TrimPrefix(rest, "/")
		switch method {
		case "PUT":
			return "CreateBackupVault", rest, "", ""
		case "GET":
			if rest == "" {
				return "ListBackupVaults", "", "", ""
			}
			return "DescribeBackupVault", rest, "", ""
		case "DELETE":
			return "DeleteBackupVault", rest, "", ""
		}
	}

	// /backup/plans[/{planId}[/selections[/{selectionId}]]]
	if strings.HasPrefix(path, "/backup/plans") {
		rest := strings.TrimPrefix(path, "/backup/plans")
		rest = strings.TrimPrefix(rest, "/")

		// /backup/plans/{planId}/selections[/{selectionId}]
		if idx := strings.Index(rest, "/selections"); idx >= 0 {
			pid := rest[:idx]
			selRest := strings.TrimPrefix(rest[idx:], "/selections")
			selRest = strings.TrimPrefix(selRest, "/")
			switch method {
			case "POST":
				return "CreateBackupSelection", "", pid, ""
			case "GET":
				return "GetBackupSelection", "", pid, selRest
			case "DELETE":
				return "DeleteBackupSelection", "", pid, selRest
			}
		}

		// /backup/plans[/{planId}]
		switch method {
		case "POST":
			if rest == "" {
				return "CreateBackupPlan", "", "", ""
			}
			return "UpdateBackupPlan", "", rest, ""
		case "GET":
			if rest == "" {
				return "ListBackupPlans", "", "", ""
			}
			return "GetBackupPlan", "", rest, ""
		case "DELETE":
			return "DeleteBackupPlan", "", rest, ""
		}
	}

	return method, "", "", ""
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
