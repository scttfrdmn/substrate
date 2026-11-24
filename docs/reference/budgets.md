# Budgets

**Endpoint:** `budgets.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSBudgetServiceGateway.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateBudget | `DuplicateRecordException` if name already exists |
| DescribeBudget | `NotFoundException` if missing |
| UpdateBudget | |
| DeleteBudget | |
| DescribeBudgets | Lists all budgets for account |

**Not modelled: `DescribeBudgetActionsForBudget`.** This table claimed it until #1015; it is
not in the dispatch. Budget *actions* — the IAM/SCP/target policies a budget can apply when a
threshold is breached — are a separate resource substrate holds no state for, so the operation
would have nothing to enumerate. `AWS::Budgets::BudgetsAction` is likewise unmodelled.

## The account, Region and creation time a record carries reach no response

`Budget` declares `AccountId`, `Region` and `CreatedAt` under wire-visible `json` tags, because the
record is what `MemoryStateManager` snapshots and a replay reads back. None of them is a member of
`API_budgets_Budget`. Until [#756](https://github.com/scttfrdmn/substrate/issues/756),
`DescribeBudget` and `DescribeBudgets` answered the record whole, so all three reached the wire. They
now answer `budgetOut` (`emulator/budgets_wire.go`), which has five published members, and the stored
record is unchanged. `TestBudgetsWire_BudgetResponsesCarryNoBookkeepingMember` drives all five routed
operations.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Budgets::Budget | BudgetName | |

## Cost

Budgets: first two budgets free, then $0.02 per budget per day.
