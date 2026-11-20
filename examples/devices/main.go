// Command devices keeps a device roster in a braid on a kolo-store directory under the first argument: a person's laptop founds a
// group and anchors its approver certificate, the phone is certified by the laptop and admitted without a new decision of a
// group admin, and then the laptop revokes it. Nothing leaves the process.
package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	kolobraid "github.com/ciranda-dev/kolo-braid"
	"github.com/ciranda-dev/kolo-braid/devices"
	koloid "github.com/ciranda-dev/kolo-id"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: devices <temp dir>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	roster, err := devices.New([]byte("devices/"), nil)
	check(err)
	apply := func(tx *kolobraid.Tx, e *kolobraid.Entry) error {
		_, err := roster.Apply(tx, e)
		return err
	}
	options := func(name string) kolobraid.Options {
		db, err := kolostore.OpenPebble(filepath.Join(os.Args[1], name))
		check(err)
		return kolobraid.Options{Backend: db, App: "devices/v1", Apply: apply}
	}

	// The person key comes from the 24 words and certifies the laptop as an approver; it is then cleared.
	phrase, err := koloid.GeneratePhrase()
	check(err)
	person, err := phrase.Person()
	check(err)
	phrase.Clear()
	personKey := person.PublicKey()
	laptop, err := koloid.Generate()
	check(err)
	offer, err := person.OfferDeviceCert(laptop.PublicKey(), &koloid.IssueOptions{Serial: 1, Approver: true})
	check(err)
	laptopCert, err := laptop.AcceptDeviceCert(offer, personKey)
	check(err)
	person.Clear()

	// The laptop founds the group and anchors its own certificate: the first device of a person is an admin's decision.
	laptopOpts := options("laptop")
	defer laptopOpts.Backend.Close()
	group, err := kolobraid.Create(ctx, laptop, kolobraid.GroupConfig{App: "devices/v1"}, laptopOpts)
	check(err)
	defer group.Close()
	check(roster.Record(ctx, group, laptopCert))
	waitAnchored(ctx, group, roster, personKey, laptop.PublicKey())
	fmt.Println("laptop: founded the group and anchored its approver certificate")

	// The laptop certifies the phone, which would come from a link-a-device invite. The phone asks to join with a proof.
	phone, err := koloid.Generate()
	check(err)
	offer, err = laptop.OfferDeviceCert(phone.PublicKey(), &koloid.IssueOptions{Parent: laptopCert, Serial: 1})
	check(err)
	phoneCert, err := phone.AcceptDeviceCert(offer, personKey)
	check(err)
	phoneOpts := options("phone")
	defer phoneOpts.Backend.Close()
	join, err := kolobraid.NewJoin(ctx, phone, phoneOpts)
	check(err)
	request, err := devices.NewJoinRequest(join, phone, phoneCert, group.GroupID(), []byte("Maria's phone"))
	check(err)
	name, err := devices.AppData(request)
	check(err)

	// The laptop admits it: the person already belongs to the group, the proof verifies and the person has room.
	welcome, err := roster.Admit(ctx, group, phone.PublicKey(), request)
	check(err)
	phoneBraid, err := join.Complete(ctx, welcome)
	check(err)
	defer phoneBraid.Close()
	fmt.Printf("laptop: admitted %q without a new decision\n", name)

	verify := func() error {
		v := group.View()
		defer v.Close()
		msg := []byte("hello")
		sig, err := phone.Sign("devices/example/v0", msg)
		check(err)
		return roster.Verify(v, personKey, phone.PublicKey(), time.Time{}, "devices/example/v0", msg, sig)
	}
	fmt.Println("the phone verifies as a device of the person:", verify() == nil)

	// The approver revokes the exact certificate it issued; every verifier that holds the record refuses the phone.
	revocation, err := laptop.RevokeDevice(&koloid.RevokeOptions{Person: personKey, Device: phone.PublicKey(), SignerCert: laptopCert, Target: phoneCert})
	check(err)
	body, err := devices.RevocationBody(revocation, phoneCert)
	check(err)
	ref, err := group.Append(ctx, body)
	check(err)
	waitSettled(ctx, group, ref)
	fmt.Println("after the laptop revokes it, the phone verifies:", verify() == nil)
}

// waitSettled waits until the entry ref of the braid is settled.
func waitSettled(ctx context.Context, b *kolobraid.Braid, ref kolobraid.EntryRef) {
	for {
		for o, err := range b.Entries(0) {
			check(err)
			if o.ID == ref.ID && o.Settled {
				return
			}
		}
		if _, err := b.Next(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

// waitAnchored waits until the settled view holds the anchored certificate of device, which Admit trusts.
func waitAnchored(ctx context.Context, b *kolobraid.Braid, roster *devices.Store, person, device ed25519.PublicKey) {
	for {
		v := b.Settled()
		ok, err := roster.Anchored(v, person, device)
		v.Close()
		if err == nil && ok {
			return
		}
		if _, err := b.Next(ctx); err != nil {
			log.Fatal(err)
		}
	}
}
