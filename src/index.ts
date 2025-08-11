import http from "http";
import express, { Request, Response, NextFunction } from "express";
import { Server } from "@colyseus/core";
import { WebSocketTransport } from "@colyseus/ws-transport";
import { DerbyRoom } from "./rooms/DerbyRoom";

const PORT = Number(process.env.PORT) || 2567;
const HOST = "0.0.0.0";

const app = express();

// Log minimale delle richieste HTTP
app.use((req: Request, _res: Response, next: NextFunction) => {
  console.log(`${new Date().toISOString()}  ${req.method} ${req.url}`);
  next();
});

// Healthcheck semplice
app.get("/healthz", (_req: Request, res: Response) => {
  console.log("HIT /healthz");
  res.status(200).send("OK");
});

// Messaggio di benvenuto sulla root
app.get("/", (_req: Request, res: Response) => {
  console.log("HIT /");
  res.status(200).send("✅ DerbyDay Colyseus server is running.");
});

// Catch-all (qualsiasi altra GET)
app.get("*", (_req: Request, res: Response) => {
  console.log("HIT catch-all");
  res.status(200).send("Default OK");
});

const httpServer = http.createServer(app);

// Server Colyseus con WebSocket
const gameServer = new Server({
  transport: new WebSocketTransport({ server: httpServer }),
});

// Definisci la stanza
gameServer.define("derby_room", DerbyRoom);

// Avvio
httpServer.listen(PORT, HOST, () => {
  console.log(`✅ Server listening on http://${HOST}:${PORT}`);
});

// (opzionale) Log errori non gestiti
process.on("unhandledRejection", (reason) => {
  console.error("UnhandledRejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("UncaughtException:", err);
});
