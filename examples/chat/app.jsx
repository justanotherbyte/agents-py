import { useAgentChat } from "@cloudflare/ai-chat/react";
import { useAgent } from "agents/react";
import { useState } from "react";
import { createRoot } from "react-dom/client";

function App() {
  const agent = useAgent({ agent: "chat" });
  const { messages, sendMessage, clearHistory, status } = useAgentChat({ agent });
  const [input, setInput] = useState("");

  function submit(event) {
    event.preventDefault();
    sendMessage({ text: input });
    setInput("");
  }

  return (
    <main>
      {messages.map((message) => (
        <p key={message.id}>
          <b>{message.role}:</b> {message.parts.map((part) => (part.type === "text" ? part.text : "")).join("")}
        </p>
      ))}
      <form onSubmit={submit}>
        <input value={input} onChange={(e) => setInput(e.target.value)} placeholder="Say something" />
        <button disabled={status === "streaming" || !input}>Send</button>
        <button type="button" onClick={clearHistory}>Clear</button>
      </form>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
