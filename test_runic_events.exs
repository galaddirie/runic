require Runic
import Runic
alias Runic.Workflow
alias Runic.Workflow.Fact

IO.puts("=" |> String.duplicate(80))
IO.puts("Testing Runic invoke_with_events behavior")
IO.puts("=" |> String.duplicate(80))

# Build a simple linear pipeline: double -> add_ten -> format
workflow =
  workflow(
    name: "test_pipeline",
    steps: [
      {step(fn x -> x * 2 end, name: :double),
       [
         {step(fn x -> x + 10 end, name: :add_ten),
          [step(fn x -> "Result: #{x}" end, name: :format)]}
       ]}
    ]
  )

IO.puts("\n📋 Workflow components:")
for {name, component} <- workflow.components do
  IO.puts("  - #{name}: hash=#{component.hash}")
end

# Plan input to get initial runnables
input_value = 5
IO.puts("\n📥 Planning input: #{input_value}")

planned_workflow = Workflow.plan_eagerly(workflow, input_value)

IO.puts("\n🔍 Initial runnables after planning:")
runnables = Workflow.next_runnables(planned_workflow)
for {node, fact} <- runnables do
  IO.puts("  - Step: #{node.name} (hash=#{node.hash})")
  IO.puts("    Input fact: value=#{inspect(fact.value)}, hash=#{fact.hash}, ancestry=#{inspect(fact.ancestry)}")
end

# Execute each step and examine events
IO.puts("\n" <> String.duplicate("=", 80))
IO.puts("Executing steps and examining invoke_with_events output")
IO.puts(String.duplicate("=", 80))

defmodule TestHelper do
  def execute_step(workflow, step_num) do
    runnables = Workflow.next_runnables(workflow)

    case runnables do
      [] ->
        IO.puts("\n✅ No more runnables - workflow complete!")
        IO.puts("\n📊 Final productions:")
        for prod <- Workflow.raw_productions(workflow) do
          IO.puts("  - #{inspect(prod)}")
        end
        workflow

      [{node, fact} | _] ->
        IO.puts("\n" <> String.duplicate("-", 60))
        IO.puts("Step #{step_num}: #{node.name}")
        IO.puts(String.duplicate("-", 60))

        IO.puts("\n  📥 Input:")
        IO.puts("    - Fact value: #{inspect(fact.value)}")
        IO.puts("    - Fact hash: #{fact.hash}")
        IO.puts("    - Fact ancestry: #{inspect(fact.ancestry)}")

        IO.puts("\n  🔧 Invoking with events...")

        # This is the key call we're testing
        {updated_workflow, events} = Workflow.invoke_with_events(workflow, node, fact)

        IO.puts("\n  📤 Results:")
        IO.puts("    - Events count: #{length(events)}")

        if Enum.empty?(events) do
          IO.puts("    - ⚠️  NO EVENTS RETURNED!")
          IO.puts("\n  🔍 Debugging - checking graph directly:")

          # Check what edges exist from this step
          out_edges = Graph.out_edges(updated_workflow.graph, node)
          produced_edges = Enum.filter(out_edges, fn e -> e.label == :produced end)

          IO.puts("    - Total out_edges from step: #{length(out_edges)}")
          IO.puts("    - :produced edges from step: #{length(produced_edges)}")

          for edge <- produced_edges do
            IO.puts("      → v2 type: #{inspect(edge.v2.__struct__)}")
            if match?(%Fact{}, edge.v2) do
              IO.puts("        value: #{inspect(edge.v2.value)}")
              IO.puts("        hash: #{edge.v2.hash}")
              IO.puts("        weight: #{edge.weight}")
            end
          end

          # Also check generations
          IO.puts("    - Workflow generations: #{updated_workflow.generations}")
        else
          for {event, idx} <- Enum.with_index(events, 1) do
            IO.puts("\n    Event #{idx}:")
            IO.puts("      - Type: #{inspect(event.__struct__)}")
            IO.puts("      - Reaction: #{inspect(event.reaction)}")
            IO.puts("      - Weight: #{event.weight}")
            IO.puts("      - From: #{inspect(event.from.__struct__)} (hash=#{Map.get(event.from, :hash, "N/A")})")
            IO.puts("      - To: #{inspect(event.to.__struct__)}")
            if match?(%Fact{}, event.to) do
              IO.puts("        → value: #{inspect(event.to.value)}")
              IO.puts("        → hash: #{event.to.hash}")
            end
          end
        end

        # Continue to next step
        execute_step(updated_workflow, step_num + 1)
    end
  end
end

TestHelper.execute_step(planned_workflow, 1)

IO.puts("\n" <> String.duplicate("=", 80))
IO.puts("Test complete!")
IO.puts(String.duplicate("=", 80))
