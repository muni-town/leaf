import { Peer, SuperPeer1, Syncer1 } from "@muni-town/leaf";
import { Name } from "./components.ts";

const superPeer = new SuperPeer1();

const peer = new Peer(new Syncer1(superPeer));

for (let i = 0; i < 100 * 1000; i++) {
  const ent = await peer.open(undefined, { awaitSync: false });

  ent.getOrInit(Name, (name) => {
    name.set("first", "Entity");
    name.set("last", i.toString());
  });
  await peer.close(ent);

  if (i % 1000 == 0)
    await new Promise((r) => {
      setTimeout(r, 0);
      console.log("i", i);
      // console.log("pause");
    });
}
