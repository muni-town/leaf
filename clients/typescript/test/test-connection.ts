#!/usr/bin/env tsx
/// <reference types="node" />

import { LeafClient } from "../src/index.js";

async function test() {
  console.log("Creating client...");
  const client = new LeafClient("http://localhost:5530", async () => "test123");

  // Enable socket.io debug logging
  client.socket.io.on("error", (err) => {
    console.log(`Manager error: ${err}`);
  });

  client.socket.on("connect_error", (err) => {
    console.log(`Connect error: ${err.message}`);
  });

  client.socket.on("connect", () => {
    console.log("Socket connected");
    console.log(`  Transport: ${client.socket.io.engine.transport.name}`);
    console.log(`  Socket ID: ${client.socket.id}`);
  });

  client.socket.on("disconnect", (reason) => {
    console.log(`Socket disconnected: ${reason}`);
  });

  client.on("authenticated", async (did) => {
    console.log(`Authenticated: ${did}`);
    console.log(`Socket connected: ${client.socket.connected}`);

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 100));

    console.log(`After delay, socket connected: ${client.socket.connected}`);

    // Try hasModule first (simpler call)
    try {
      console.log("Calling hasModule...");
      const hasIt = await client.hasModule(
        "95c469822e1a1cc1b91efb049c8833dc025ce3cd5b9658a22db09cb4197ff7a3",
      );
      console.log(`hasModule result: ${hasIt}`);
    } catch (error) {
      console.error("hasModule error:", error);
    }

    // Try to get stream info
    try {
      console.log("Calling streamInfo...");
      console.log(`  Socket connected before call: ${client.socket.connected}`);
      console.log(`  Socket disconnected: ${client.socket.disconnected}`);

      const info = await client.streamInfo(
        "95c469822e1a1cc1b91efb049c8833dc025ce3cd5b9658a22db09cb4197ff7a3",
      );
      console.log("Success!", info);
    } catch (error) {
      console.error("Error:", error);
      console.log(`  Socket connected after error: ${client.socket.connected}`);
      console.log(
        `  Socket disconnected after error: ${client.socket.disconnected}`,
      );
    } finally {
      client.disconnect();
      process.exit(0);
    }
  });
}

test().catch(console.error);
