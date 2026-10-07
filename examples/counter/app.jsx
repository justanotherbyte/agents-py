import { useAgent } from "agents/react";
import { createRoot } from "react-dom/client";

function App() {
  const agent = useAgent({ agent: "counter" });

  return (
    <main>
      <h1>{agent.state?.count ?? 0}</h1>
      <button onClick={() => agent.stub.increment(1)}>+1</button>
      <button onClick={() => agent.setState({ count: 0 })}>Reset</button>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
