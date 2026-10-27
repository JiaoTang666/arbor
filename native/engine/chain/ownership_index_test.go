package chain

import "testing"

func TestOwnershipSecuritySharesHistoricalIndex(t *testing.T) {
	bc := ownershipSecurityFixture(t)(0)
	if bc.initialLedger.Routes != nil || len(bc.PartitionMap) != 0 {
		t.Fatal("security bootstrap retained duplicate historical route maps")
	}
	first, err := bc.initialLedger.SealRoutes()
	if err != nil {
		t.Fatal(err)
	}
	second, err := bc.initialLedger.SealRoutes()
	if err != nil || first != second || first.Len() != 1 {
		t.Fatal("sealed base rebuilt or changed")
	}
	if owner, ok := bc.initialLedger.HistoricalOwner(securityOldAccount); !ok || owner != 0 {
		t.Fatal("historical provenance lookup lost genesis")
	}
	if bc.Get_PartitionMap(securityOldAccount) != 0 || bc.Get_PartitionMap(securityRemoteNew) != 1 {
		t.Fatal("historical or newly authorized routing changed")
	}
	bc.Update_PartitionMap(securityOldAccount, 0)
	bc.Update_PartitionMap(securityRemoteNew, 1)
	if len(bc.PartitionMap) != 0 {
		t.Fatal("authorized no-op update rebuilt a redundant route table")
	}
	if local, total := bc.snapshotPartitionCounts(0); local != 2 || total != 4 {
		t.Fatalf("logical ownership counts differ: %d/%d", local, total)
	}
	requireSecurityPanic(t, func() { bc.Update_PartitionMap(securityOldAccount, 1) })
	requireSecurityPanic(t, func() { bc.Get_PartitionMap("ffffffffffffffffffffffffffffffffffffffff") })
}
