import { useAgent } from "agents/react";
import { useState } from "react";
import { createRoot } from "react-dom/client";

function App() {
  const agent = useAgent({ agent: "reminders" });
  const [text, setText] = useState("");
  const { pending = [], done = [] } = agent.state ?? {};

  function submit(event) {
    event.preventDefault();
    agent.stub.remind(text, 5);
    setText("");
  }

  return (
    <main>
      <form onSubmit={submit}>
        <input value={text} onChange={(e) => setText(e.target.value)} placeholder="Remind me in 5 seconds to..." />
        <button disabled={!text}>Add</button>
      </form>
      <h2>Pending</h2>
      <ul>{pending.map((item, i) => <li key={i}>{item}</li>)}</ul>
      <h2>Done</h2>
      <ul>{done.map((item, i) => <li key={i}>{item}</li>)}</ul>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
