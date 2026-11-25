package emulator

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
)

// A subnet's private IPv4 addresses (#1592).
//
// Every address substrate hands out — an instance's primary interface, a secondary interface,
// a network interface CreateNetworkInterface makes, a NAT gateway — comes from the subnet it is
// in, through one allocation record per subnet, so two resources in a subnet never report the
// same address and every address lies inside the subnet's CIDR block.
//
// The VPC User Guide's "Subnet CIDR blocks" fixes which addresses are usable: "The first four
// IP addresses and the last IP address in each subnet CIDR block are not available for your use,
// and they cannot be assigned to a resource, such as an EC2 instance." In a 10.0.0.0/24 those
// are 10.0.0.0 to 10.0.0.3 and 10.0.0.255, so a /24 has 251 usable addresses and a /28 has 11.
//
// # Allocation order
//
// An automatic address is the next free one after the last address handed out, wrapping from
// the top of the block back to the fifth address. A released address is therefore reused only
// once the cursor has gone round the block, not immediately: a test that terminates an instance
// and launches another sees the new instance at a new address, as it usually does on AWS, rather
// than at the one the terminated instance still reports. AWS documents no order at all, so the
// cursor is substrate's reading; what it guarantees is uniqueness, range and replay — the same
// requests in the same order give the same addresses, because the record lives in state and
// every allocation is a read-modify-write of it under [EC2Plugin.addrMu].
//
// # Explicit addresses
//
// An address the request names is checked in the order AWS reports: one outside the block, one
// in the five reserved addresses, then one already held. The first two are InvalidParameterValue
// and the third InvalidIPAddress.InUse, each with EC2's observed message (see the error
// constructors). Naming an address does not move the cursor.
//
// # A subnet with no allocation record
//
// A subnet with no allocation record — every subnet in state an older build wrote, and a new one
// until its first allocation — has its record built on first use from what the subnet already
// holds: the network interfaces stored in it, the unstored interfaces of its live instances (an
// older build recorded those on the instance), and its NAT gateways not yet deleted. An address
// such a resource holds is not handed out again, and an older instance keeps the address it
// recorded. An address outside the block, which an older build could record, holds nothing in it.
// Older builds could also record one address on several instances of a subnet; it is held under
// the first, and a release passes it to the next resource still reporting it (see
// [EC2Plugin.otherReporter]).

// ec2AddressBookKey is the state key of one subnet's allocation record.
func ec2AddressBookKey(accountID, region, subnetID string) string {
	return "ipalloc:" + accountID + "/" + region + "/" + subnetID
}

// ec2AddressBook is one subnet's allocation record: the block it allocates from, the cursor the
// next automatic address starts at, and every address held, with its holder.
type ec2AddressBook struct {
	// CIDR is the block the record allocates from, the subnet's IPv4 CIDR block.
	CIDR string `json:"cidr"`

	// Next is the offset into the block of the first address the next automatic allocation
	// tries.
	Next uint64 `json:"next"`

	// InUse maps each held address to the ID of the resource holding it: a network interface,
	// a NAT gateway, or the instance itself for an instance whose interfaces predate standalone
	// interface records.
	InUse map[string]string `json:"in_use"`

	// Unconstrained records a subnet with no parseable IPv4 CIDR block, which allocates from
	// [ec2FallbackSubnetCIDR] and accepts any IPv4 address a request names.
	Unconstrained bool `json:"unconstrained,omitempty"`
}

// ec2FallbackSubnetCIDR is the block a subnet with no parseable IPv4 CIDR block allocates from.
// CreateSubnet does not require one, and such a subnet still needs addresses for what is launched
// into it; the default VPC's block is the one an account has whatever else it creates. Only the
// range is assumed — uniqueness still holds — and since the subnet states no range, an address a
// request names is not held to this one.
const ec2FallbackSubnetCIDR = "172.31.0.0/16"

// ec2Block is an IPv4 block, its base address and size.
type ec2Block struct {
	base uint64
	size uint64
}

// ec2ParseBlock parses an IPv4 CIDR block, falling back to [ec2FallbackSubnetCIDR].
func ec2ParseBlock(cidr string) ec2Block {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil || network.IP.To4() == nil {
		_, network, _ = net.ParseCIDR(ec2FallbackSubnetCIDR) //nolint:errcheck // A constant that parses.
	}
	ones, bits := network.Mask.Size()
	return ec2Block{
		base: uint64(binary.BigEndian.Uint32(network.IP.To4())),
		size: uint64(1) << uint(bits-ones),
	}
}

// firstUsable and lastUsable bound the usable offsets: the first four and the last address are
// reserved.
func (b ec2Block) firstUsable() uint64 { return 4 }
func (b ec2Block) lastUsable() uint64  { return b.size - 2 }

// usable is the number of addresses the block can hand out, zero for a block too small to have
// any after the five reserved.
func (b ec2Block) usable() uint64 {
	if b.size < 6 {
		return 0
	}
	return b.size - 5
}

// address renders the address at offset off.
func (b ec2Block) address(off uint64) string {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, uint32(b.base+off))
	return ip.String()
}

// offset reports addr's offset into the block, and whether addr is an IPv4 address inside it.
func (b ec2Block) offset(addr string) (uint64, bool) {
	ip := net.ParseIP(addr)
	if ip == nil || ip.To4() == nil {
		return 0, false
	}
	n := uint64(binary.BigEndian.Uint32(ip.To4()))
	if n < b.base || n >= b.base+b.size {
		return 0, false
	}
	return n - b.base, true
}

// available reports how many addresses the record can still hand out, the value
// DescribeSubnets reports as availableIpAddressCount.
func (book *ec2AddressBook) available() int {
	b := ec2ParseBlock(book.CIDR)
	held := 0
	for addr := range book.InUse {
		if off, ok := b.offset(addr); ok && off >= b.firstUsable() && off <= b.lastUsable() {
			held++
		}
	}
	return int(b.usable()) - held
}

// ec2AddressNotIPv4Error refuses a PrivateIpAddress that is not an IPv4 address. The message is
// substrate's own, in the shape of [ec2InvalidParameterValue].
func ec2AddressNotIPv4Error(addr string) *AWSError {
	return ec2InvalidParameterValue("PrivateIpAddress", addr, "It is not an IPv4 address.")
}

// ec2AddressOutOfRangeError refuses an address outside the subnet's block. The code and message are
// EC2's observed answer to RunInstances and CreateNetworkInterface, "InvalidParameterValue: Address
// does not fall within the subnet's address range" (saltstack/salt#40211, a captured
// CreateNetworkInterface response; Stack Overflow 74649048, from the Terraform AWS provider); no
// AWS page publishes it.
func ec2AddressOutOfRangeError() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Address does not fall within the subnet's address range",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2AddressReservedError refuses one of the five addresses a subnet reserves. The message is
// EC2's observed RunInstances answer, `Address "<ip>" is in subnet's reserved address range`, as
// AWS re:Post's "Common errors while launching drill/recovery/test instance" article quotes it.
func ec2AddressReservedError(addr string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Address " + addr + " is in subnet's reserved address range",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2AddressInUseError refuses an address another resource holds: InvalidIPAddress.InUse, the
// client-error table's "The specified IP address is already in use.", with EC2's observed message
// "Address 10.0.0.12 is in use." (hashicorp/terraform-provider-aws#17431,
// ansible/ansible-modules-core#835).
func ec2AddressInUseError(addr string) *AWSError {
	return &AWSError{
		Code:       "InvalidIPAddress.InUse",
		Message:    "Address " + addr + " is in use.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2InsufficientAddressesError refuses an allocation the subnet has no free address for, with the
// client-error table's InsufficientFreeAddressesInSubnet and its gloss's first sentence.
func ec2InsufficientAddressesError() *AWSError {
	return &AWSError{
		Code:       "InsufficientFreeAddressesInSubnet",
		Message:    "The specified subnet does not contain enough free private IP addresses to fulfill your request.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2AddrTxn is one request's allocations across the subnets it touches. Allocations are made
// against the loaded records in memory and written by [ec2AddrTxn.commit], so a request that is
// refused part-way — the second instance of a launch naming one address, say — writes nothing.
//
// The transaction holds [EC2Plugin.addrMu] from [EC2Plugin.beginAddrTxn] until
// [ec2AddrTxn.close]. A caller that also holds sgMu takes it first.
type ec2AddrTxn struct {
	p     *EC2Plugin
	ctx   context.Context
	acct  string
	reg   string
	books map[string]*ec2AddressBook
	dirty []string
	// owned reports a transaction that took addrMu itself and releases it on close.
	owned bool
}

// beginAddrTxn starts an allocation transaction for the request's account and Region.
func (p *EC2Plugin) beginAddrTxn(reqCtx *RequestContext) *ec2AddrTxn {
	p.addrMu.Lock()
	t := p.addrTxnLocked(reqCtx)
	t.owned = true
	return t
}

// addrTxnLocked starts an allocation transaction for a caller that already holds addrMu.
func (p *EC2Plugin) addrTxnLocked(reqCtx *RequestContext) *ec2AddrTxn {
	return &ec2AddrTxn{
		p: p, ctx: context.Background(), acct: reqCtx.AccountID, reg: reqCtx.Region,
		books: map[string]*ec2AddressBook{},
	}
}

// close releases the transaction's lock, when it took it. Uncommitted allocations are discarded.
func (t *ec2AddrTxn) close() {
	if t.owned {
		t.p.addrMu.Unlock()
	}
}

// book returns subnet's allocation record, loading it or building it from what the subnet holds.
func (t *ec2AddrTxn) book(subnet EC2Subnet) (*ec2AddressBook, error) {
	if b, ok := t.books[subnet.SubnetID]; ok {
		return b, nil
	}
	b, err := t.p.loadAddressBook(t.ctx, t.acct, t.reg, subnet)
	if err != nil {
		return nil, err
	}
	t.books[subnet.SubnetID] = b
	return b, nil
}

// markDirty records that subnetID's record changed.
func (t *ec2AddrTxn) markDirty(subnetID string) {
	if !slices.Contains(t.dirty, subnetID) {
		t.dirty = append(t.dirty, subnetID)
	}
}

// allocate hands holder an address in subnet: explicit when the request named one, the next free
// one otherwise. A refusal is returned as an *AWSError.
func (t *ec2AddrTxn) allocate(subnet EC2Subnet, explicit, holder string) (string, error) {
	book, err := t.book(subnet)
	if err != nil {
		return "", err
	}
	b := ec2ParseBlock(book.CIDR)
	if explicit != "" {
		if ip := net.ParseIP(explicit); ip == nil || ip.To4() == nil {
			return "", ec2AddressNotIPv4Error(explicit)
		}
		off, inBlock := b.offset(explicit)
		switch {
		case book.Unconstrained:
		case !inBlock:
			return "", ec2AddressOutOfRangeError()
		case b.usable() == 0 || off < b.firstUsable() || off > b.lastUsable():
			return "", ec2AddressReservedError(explicit)
		}
		if _, held := book.InUse[explicit]; held {
			return "", ec2AddressInUseError(explicit)
		}
		book.InUse[explicit] = holder
		t.markDirty(subnet.SubnetID)
		return explicit, nil
	}
	if b.usable() == 0 {
		return "", ec2InsufficientAddressesError()
	}
	start := book.Next
	if start < b.firstUsable() || start > b.lastUsable() {
		start = b.firstUsable()
	}
	for i := uint64(0); i < b.usable(); i++ {
		off := b.firstUsable() + (start-b.firstUsable()+i)%b.usable()
		addr := b.address(off)
		if _, held := book.InUse[addr]; held {
			continue
		}
		book.InUse[addr] = holder
		book.Next = off + 1
		t.markDirty(subnet.SubnetID)
		return addr, nil
	}
	return "", ec2InsufficientAddressesError()
}

// setHolder names the holder of an address allocated before its holder's ID was minted.
func (t *ec2AddrTxn) setHolder(subnetID, addr, holder string) {
	if book, ok := t.books[subnetID]; ok {
		if _, held := book.InUse[addr]; held {
			book.InUse[addr] = holder
			t.markDirty(subnetID)
		}
	}
}

// release frees addrs in subnetID that one of holders holds. An address another resource now
// holds is left alone, so a release that runs twice cannot free someone else's address, and one
// another resource still reports passes to it ([EC2Plugin.otherReporter]). A subnet that is no
// longer stored has nothing to release.
func (t *ec2AddrTxn) release(subnetID string, addrs []string, holders ...string) error {
	if len(addrs) == 0 {
		return nil
	}
	book, err := t.bookByID(subnetID)
	if err != nil || book == nil {
		return err
	}
	for _, addr := range addrs {
		h, held := book.InUse[addr]
		if !held || !slices.Contains(holders, h) {
			continue
		}
		next, err := t.p.otherReporter(t.ctx, t.acct, t.reg, subnetID, addr, holders)
		if err != nil {
			return err
		}
		if next != "" {
			book.InUse[addr] = next
		} else {
			delete(book.InUse, addr)
		}
		t.markDirty(subnetID)
	}
	return nil
}

// transferIf passes addr in subnetID to to, when its holder is one from accepts.
func (t *ec2AddrTxn) transferIf(subnetID, addr string, from func(holder string) bool, to string) error {
	book, err := t.bookByID(subnetID)
	if err != nil || book == nil {
		return err
	}
	if h, held := book.InUse[addr]; held && from(h) {
		book.InUse[addr] = to
		t.markDirty(subnetID)
	}
	return nil
}

// bookByID returns the allocation record of the subnet subnetID names, or nil when that subnet
// is no longer stored.
func (t *ec2AddrTxn) bookByID(subnetID string) (*ec2AddressBook, error) {
	if subnetID == "" {
		return nil, nil
	}
	if b, ok := t.books[subnetID]; ok {
		return b, nil
	}
	data, err := t.p.state.Get(t.ctx, ec2Namespace, "subnet:"+t.acct+"/"+t.reg+"/"+subnetID)
	if err != nil {
		return nil, fmt.Errorf("ec2 address book get subnet: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var subnet EC2Subnet
	if err := json.Unmarshal(data, &subnet); err != nil {
		return nil, fmt.Errorf("ec2 address book unmarshal subnet: %w", err)
	}
	return t.book(subnet)
}

// commit writes every record the transaction changed.
func (t *ec2AddrTxn) commit() error {
	for _, subnetID := range t.dirty {
		if err := t.p.putEC2Record(t.ctx, ec2AddressBookKey(t.acct, t.reg, subnetID), t.books[subnetID]); err != nil {
			return fmt.Errorf("ec2 address book: %w", err)
		}
	}
	t.dirty = nil
	return nil
}

// loadAddressBook reads subnet's allocation record, or builds it from what the subnet already
// holds when none is stored. The built record is not written here: a describe that reads it
// leaves state as it found it, and the first allocation writes it.
func (p *EC2Plugin) loadAddressBook(ctx context.Context, acct, region string, subnet EC2Subnet) (*ec2AddressBook, error) {
	data, err := p.state.Get(ctx, ec2Namespace, ec2AddressBookKey(acct, region, subnet.SubnetID))
	if err != nil {
		return nil, fmt.Errorf("ec2 address book get: %w", err)
	}
	if data != nil {
		var book ec2AddressBook
		if err := json.Unmarshal(data, &book); err != nil {
			return nil, fmt.Errorf("ec2 address book unmarshal: %w", err)
		}
		if book.InUse == nil {
			book.InUse = map[string]string{}
		}
		return &book, nil
	}
	book := &ec2AddressBook{CIDR: subnet.CIDRBlock, InUse: map[string]string{}}
	if _, network, err := net.ParseCIDR(subnet.CIDRBlock); err != nil || network.IP.To4() == nil {
		book.CIDR = ec2FallbackSubnetCIDR
		book.Unconstrained = true
	}
	b := ec2ParseBlock(book.CIDR)
	book.Next = b.firstUsable()
	hold := func(addr, holder string) {
		if off, ok := b.offset(addr); ok && off >= b.firstUsable() && off <= b.lastUsable() {
			if _, held := book.InUse[addr]; !held {
				book.InUse[addr] = holder
			}
		}
	}
	scope := acct + "/" + region + "/"
	materialized, err := p.ec2LegacyMaterialized(ctx, acct, region)
	if err != nil {
		return nil, err
	}
	if err := ec2ScanRecords(ctx, p.state, "eni:"+scope, func(_ string, eni EC2NetworkInterfaceRecord) {
		if eni.SubnetID != subnet.SubnetID {
			return
		}
		for _, addr := range eni.addresses() {
			hold(addr, eni.NetworkInterfaceID)
		}
	}); err != nil {
		return nil, fmt.Errorf("ec2 address book network interfaces: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "instance:"+scope, func(_ string, inst EC2Instance) {
		if inst.State.Name == ec2StateTerminated || inst.InterfacesStandalone || materialized[inst.InstanceID] {
			return
		}
		for _, eni := range ec2LegacyInterfaces(inst) {
			if eni.SubnetID == subnet.SubnetID {
				hold(eni.PrivateIPAddress, inst.InstanceID)
			}
		}
	}); err != nil {
		return nil, fmt.Errorf("ec2 address book instances: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "nat:"+scope, func(_ string, gw EC2NATGateway) {
		if gw.SubnetID == subnet.SubnetID && gw.State != "deleted" {
			hold(gw.PrivateIP, gw.NatGatewayID)
		}
	}); err != nil {
		return nil, fmt.Errorf("ec2 address book NAT gateways: %w", err)
	}
	return book, nil
}

// deleteAddressBook removes a deleted subnet's allocation record, which DeleteSubnet refuses to
// reach while anything still holds an address in it.
func (p *EC2Plugin) deleteAddressBook(reqCtx *RequestContext, subnetID string) error {
	p.addrMu.Lock()
	defer p.addrMu.Unlock()
	if err := p.state.Delete(context.Background(), ec2Namespace, ec2AddressBookKey(reqCtx.AccountID, reqCtx.Region, subnetID)); err != nil {
		return fmt.Errorf("ec2 deleteSubnet address book: %w", err)
	}
	return nil
}

// subnetAvailableAddresses reports subnet's availableIpAddressCount.
func (p *EC2Plugin) subnetAvailableAddresses(ctx context.Context, subnet EC2Subnet) (int, error) {
	p.addrMu.Lock()
	defer p.addrMu.Unlock()
	book, err := p.loadAddressBook(ctx, subnet.AccountID, subnet.Region, subnet)
	if err != nil {
		return 0, err
	}
	return book.available(), nil
}

// ec2SubnetItemFor renders subnet with its availableIpAddressCount, which is read from the
// subnet's allocation record.
func (p *EC2Plugin) ec2SubnetItemFor(ctx context.Context, subnet EC2Subnet) (ec2SubnetItem, error) {
	item := ec2SubnetXML(subnet)
	n, err := p.subnetAvailableAddresses(ctx, subnet)
	if err != nil {
		return item, err
	}
	item.AvailableIPAddressCount = n
	return item, nil
}
