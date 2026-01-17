defprotocol Runic.Workflow.Invokable do
  @moduledoc """
  Protocol enforcing how an operation/step/node within a workflow can always be activated in context of a workflow.

  The return of an implementation's `invoke/3` should always return a new workflow.

  The invocable protocol is invoked to evaluate valid steps in that cycle starting with conditionals.
  """
  def invoke(node, workflow, fact)
  def match_or_execute(node)

  # replay reduces facts into a workflow to rebuild the workflow's memory from a log
  # in contrast to invoke which receives a runnable; replay instead accepts the fact the invokable has produced before
  # i.e. the fact ancestry hash == node.hash
  # this lets the protocol know what edges to draw in the workflow memory
  # def replay(node, workflow, fact)
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.Root do
  alias Runic.Workflow.Root
  alias Runic.Workflow.Fact
  alias Runic.Workflow

  # def invoke(%Root{}, workflow, %Fact{ancestry: {parent_hash, _parent_fact_hash}} = fact) do
  #   workflow
  #   |> Workflow.log_fact(fact)
  #   |> Workflow.prepare_next_runnables(Map.get(workflow.graph.vertices, parent_hash), fact)
  # end

  def invoke(%Root{} = root, workflow, fact) do
    workflow
    |> Workflow.log_fact(fact)
    |> Workflow.prepare_next_generation(fact)
    |> Workflow.prepare_next_runnables(root, fact)
  end

  def match_or_execute(_root), do: :match
  # def runnable_connection(_root), do: :root
  # def resolved_connection(_root), do: :root

  # def replay(_root, workflow, fact) do
  #   workflow
  #   |> Workflow.log_fact(fact)
  #   |> Workflow.prepare_next_generation(fact)
  # end
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.Condition do
  alias Runic.Workflow

  alias Runic.Workflow.{
    Fact,
    Condition,
    Components
  }

  require Logger

  @spec invoke(Runic.Workflow.Condition.t(), Runic.Workflow.t(), Runic.Workflow.Fact.t()) ::
          Runic.Workflow.t()
  def invoke(
        %Condition{} = condition,
        %Workflow{} = workflow,
        %Fact{} = fact
      ) do
    if try_to_run_work(condition.work, fact.value, condition.arity) do
      workflow
      |> Workflow.prepare_next_runnables(condition, fact)
      |> Workflow.draw_connection(fact, condition, :satisfied)
      |> Workflow.mark_runnable_as_ran(condition, fact)
    else
      Workflow.mark_runnable_as_ran(workflow, condition, fact)
    end
  end

  # def runnable_connection(_condition), do: :matchable
  # def resolved_connection(_condition), do: :satisfied

  def match_or_execute(_condition), do: :match

  # def replay(condition, workflow, fact) do
  #   workflow
  #   |> Workflow.prepare_next_runnables(condition, fact)
  #   |> Workflow.draw_connection(fact, condition, :satisfied)
  #   |> Workflow.mark_runnable_as_ran(condition, fact)
  # end

  defp try_to_run_work(work, fact_value, arity) do
    try do
      run_work(work, fact_value, arity)
    rescue
      FunctionClauseError -> false
      BadArityError -> false
    catch
      true ->
        true

      any ->
        Logger.error(
          "something other than FunctionClauseError happened in try_to_run_work/3: \n\n #{inspect(any)}"
        )

        false
    end
  end

  defp run_work(work, fact_value, 1) when is_list(fact_value) do
    apply(work, fact_value)
  end

  defp run_work(work, fact_value, arity) when arity > 1 and is_list(fact_value) do
    Components.run(work, fact_value)
  end

  defp run_work(_work, _fact_value, arity) when arity > 1 do
    false
  end

  defp run_work(work, fact_value, _arity) do
    Components.run(work, fact_value)
  end
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.Step do
  alias Runic.Workflow
  alias Runic.Workflow.{Fact, Step, Components}

  @spec invoke(%Runic.Workflow.Step{}, Runic.Workflow.t(), Runic.Workflow.Fact.t()) ::
          Runic.Workflow.t()
  def invoke(
        %Step{} = step,
        %Workflow{} = workflow,
        %Fact{} = fact
      ) do
    result = Components.run(step.work, fact.value, Components.arity_of(step.work))

    # Propagate item_index and items_total from input fact to output fact
    # This allows joins to correctly pair items even after multiple transformations
    result_fact =
      Fact.new(
        value: result,
        ancestry: {step.hash, fact.hash},
        item_index: fact.item_index,
        items_total: fact.items_total,
        fan_out_hash: fact.fan_out_hash
      )

    causal_generation = Workflow.causal_generation(workflow, fact)

    workflow
    |> Workflow.log_fact(result_fact)
    |> Workflow.draw_connection(step, result_fact, :produced, weight: causal_generation)
    |> Workflow.mark_runnable_as_ran(step, fact)
    |> Workflow.run_after_hooks(step, result_fact)
    |> Workflow.prepare_next_runnables(step, result_fact)
    |> maybe_prepare_map_reduce(step, result_fact)
  end

  def match_or_execute(_step), do: :execute

  defp is_reduced_in_map?(workflow, step) do
    MapSet.member?(workflow.mapped.mapped_paths, step.hash)
  end

  defp maybe_prepare_map_reduce(workflow, step, fact) do
    if is_reduced_in_map?(workflow, step) do
      key = {workflow.generations, step.hash}
      sister_facts = workflow.mapped[key] || []

      Map.put(workflow, :mapped, Map.put(workflow.mapped, key, [fact.hash | sister_facts]))
    else
      workflow
    end
  end
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.Conjunction do
  alias Runic.Workflow

  alias Runic.Workflow.{
    Fact,
    Conjunction
  }

  @spec invoke(%Runic.Workflow.Conjunction{}, Runic.Workflow.t(), Runic.Workflow.Fact.t()) ::
          Runic.Workflow.t()
  def invoke(
        %Conjunction{} = conj,
        %Workflow{} = workflow,
        %Fact{} = fact
      ) do
    satisfied_conditions = Workflow.satisfied_condition_hashes(workflow, fact)

    causal_generation = Workflow.causal_generation(workflow, fact)

    if conj.hash not in satisfied_conditions and
         Enum.all?(conj.condition_hashes, &(&1 in satisfied_conditions)) do
      workflow
      |> Workflow.prepare_next_runnables(conj, fact)
      |> Workflow.draw_connection(fact, conj, :satisfied, weight: causal_generation)
      |> Workflow.mark_runnable_as_ran(conj, fact)
    else
      Workflow.mark_runnable_as_ran(workflow, conj, fact)
    end
  end

  def match_or_execute(_conjunction), do: :match
  # def runnable_connection(_conjunction), do: :matchable
  # def resolved_connection(_conjunction), do: :satisfied
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.MemoryAssertion do
  alias Runic.Workflow

  alias Runic.Workflow.{
    Fact,
    MemoryAssertion
  }

  @spec invoke(
          %Runic.Workflow.MemoryAssertion{},
          Runic.Workflow.t(),
          Runic.Workflow.Fact.t()
        ) :: Runic.Workflow.t()
  def invoke(
        %MemoryAssertion{} = ma,
        %Workflow{} = workflow,
        %Fact{} = fact
      ) do
    if ma.memory_assertion.(workflow, fact) do
      causal_generation = Workflow.causal_generation(workflow, fact)

      workflow
      |> Workflow.draw_connection(fact, ma, :satisfied, weight: causal_generation)
      |> Workflow.mark_runnable_as_ran(ma, fact)
      |> Workflow.prepare_next_runnables(ma, fact)
    else
      Workflow.mark_runnable_as_ran(workflow, ma, fact)
    end
  end

  def match_or_execute(_memory_assertion), do: :match
  # def runnable_connection(_memory_assertion), do: :matchable
  # def resolved_connection(_memory_assertion), do: :satisfied
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.StateReaction do
  alias Runic.Workflow

  alias Runic.Workflow.{
    Fact,
    Components,
    StateReaction
  }

  def invoke(
        %StateReaction{} = sr,
        %Workflow{} = workflow,
        %Fact{} = fact
      ) do
    last_known_state = Workflow.last_known_state(workflow, sr)

    result = Components.run(sr.work, last_known_state, sr.arity)

    unless result == {:error, :no_match_of_lhs_in_reactor_fn} do
      result_fact = Fact.new(value: result, ancestry: {sr.hash, fact.hash})

      causal_generation = Workflow.causal_generation(workflow, fact)

      workflow
      |> Workflow.log_fact(result_fact)
      |> Workflow.draw_connection(sr, result_fact, :produced, weight: causal_generation)
      |> Workflow.run_after_hooks(sr, result_fact)
      |> Workflow.prepare_next_runnables(sr, result_fact)
      |> Workflow.mark_runnable_as_ran(sr, fact)
    else
      Workflow.mark_runnable_as_ran(workflow, sr, fact)
    end
  end

  def match_or_execute(_state_reaction), do: :execute
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.StateCondition do
  alias Runic.Workflow

  alias Runic.Workflow.{
    Fact,
    StateCondition
  }

  @spec invoke(
          %Runic.Workflow.StateCondition{},
          Runic.Workflow.t(),
          Runic.Workflow.Fact.t()
        ) :: Runic.Workflow.t()
  def invoke(
        %StateCondition{} = sc,
        %Workflow{} = workflow,
        %Fact{} = fact
      ) do
    last_known_state = Workflow.last_known_state(workflow, sc)

    # check
    sc_result = sc.work.(fact.value, last_known_state)

    if sc_result do
      causal_generation = Workflow.causal_generation(workflow, fact)

      workflow
      |> Workflow.prepare_next_runnables(sc, fact)
      |> Workflow.draw_connection(fact, sc, :satisfied, weight: causal_generation)
      |> Workflow.mark_runnable_as_ran(sc, fact)
    else
      Workflow.mark_runnable_as_ran(workflow, sc, fact)
    end
  end

  def match_or_execute(_state_condition), do: :match
  # def runnable_connection(_state_condition), do: :matchable
  # def resolved_connection(_state_condition), do: :satisfied
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.Accumulator do
  alias Runic.Workflow

  alias Runic.Workflow.{
    Fact,
    Components,
    Accumulator
  }

  @spec invoke(%Runic.Workflow.Accumulator{}, Runic.Workflow.t(), Runic.Workflow.Fact.t()) ::
          Runic.Workflow.t()
  def invoke(
        %Accumulator{} = acc,
        %Workflow{} = workflow,
        %Fact{} = fact
      ) do
    last_known_state = last_known_state(workflow, acc)

    unless is_nil(last_known_state) do
      next_state = apply(acc.reducer, [fact.value, last_known_state.value])

      next_state_produced_fact = Fact.new(value: next_state, ancestry: {acc.hash, fact.hash})

      causal_generation = Workflow.causal_generation(workflow, fact)

      workflow
      |> Workflow.log_fact(next_state_produced_fact)
      |> Workflow.draw_connection(acc, fact, :state_produced, weight: causal_generation)
      |> Workflow.mark_runnable_as_ran(acc, fact)
      |> Workflow.run_after_hooks(acc, next_state_produced_fact)
      |> Workflow.prepare_next_runnables(acc, fact)
    else
      init_fact = init_fact(acc)

      next_state = apply(acc.reducer, [fact.value, init_fact.value])

      next_state_produced_fact = Fact.new(value: next_state, ancestry: {acc.hash, fact.hash})

      causal_generation = Workflow.causal_generation(workflow, fact)

      workflow
      |> Workflow.log_fact(init_fact)
      |> Workflow.draw_connection(acc, init_fact, :state_initiated, weight: causal_generation)
      |> Workflow.log_fact(next_state_produced_fact)
      |> Workflow.draw_connection(acc, next_state_produced_fact, :state_produced,
        weight: causal_generation
      )
      |> Workflow.mark_runnable_as_ran(acc, fact)
      |> Workflow.run_after_hooks(acc, next_state_produced_fact)
      |> Workflow.prepare_next_runnables(acc, fact)
    end
  end

  def match_or_execute(_state_reactor), do: :execute

  # def runnable_connection(_state_condition), do: :runnable
  # def resolved_connection(_state_condition), do: :state_produced

  defp last_known_state(workflow, accumulator) do
    workflow.graph
    |> Graph.out_edges(accumulator, by: :state_produced)
    |> List.first(%{})
    |> Map.get(:v2)
  end

  defp init_fact(%Accumulator{init: init, hash: hash}) do
    init = init.()
    Fact.new(value: init, ancestry: {hash, Components.fact_hash(init)})
  end
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.Join do
  alias Runic.Workflow
  alias Runic.Workflow.{Fact, Join}

  @spec invoke(%Runic.Workflow.Join{}, Runic.Workflow.t(), Runic.Workflow.Fact.t()) ::
          Runic.Workflow.t()
  def invoke(
        %Join{} = join,
        %Workflow{} = workflow,
        %Fact{ancestry: {_parent_hash, _value_hash}} = fact
      ) do
    causal_generation = Workflow.causal_generation(workflow, fact)

    workflow =
      Workflow.draw_connection(workflow, fact, join, :joined, weight: causal_generation)

    if Join.fan_out_aware?(join) do
      invoke_fan_out_aware(join, workflow, fact, causal_generation)
    else
      invoke_standard(join, workflow, fact, causal_generation)
    end
  end

  # Standard join: wait for one fact from each parent branch
  defp invoke_standard(join, workflow, fact, causal_generation) do
    join_order_weights =
      join.joins
      |> Enum.with_index()
      |> Map.new()

    # Get all joined facts for this join
    possible_priors =
      workflow.graph
      |> Graph.in_edges(join)
      |> Enum.filter(&(&1.label == :joined))
      |> Enum.sort(fn edge1, edge2 ->
        join_weight_1 = Map.get(join_order_weights, elem(edge1.v1.ancestry, 0))
        join_weight_2 = Map.get(join_order_weights, elem(edge2.v1.ancestry, 0))
        join_weight_1 <= join_weight_2
      end)
      |> Enum.map(& &1.v1.value)

    if Enum.count(join.joins) == Enum.count(possible_priors) do
      produce_join_result(join, workflow, fact, possible_priors, causal_generation)
    else
      workflow
    end
  end

  # Fan-out aware join: collect all items from each branch, then apply join mode
  defp invoke_fan_out_aware(join, workflow, fact, causal_generation) do
    # Group joined facts by their originating branch (parent step hash)
    joined_edges =
      workflow.graph
      |> Graph.in_edges(join)
      |> Enum.filter(&(&1.label == :joined))

    # Group facts by their parent step hash
    facts_by_branch =
      Enum.group_by(joined_edges, fn edge ->
        {parent_hash, _} = edge.v1.ancestry
        parent_hash
      end)

    # Check if we have facts from all branches
    # For fan-out aware joins, we need to check if all fan-out branches have completed
    all_branches_ready? = check_all_branches_ready(join, workflow, facts_by_branch)

    if all_branches_ready? do
      # Collect values from each branch in join order, sorted by item_index
      branch_values =
        Enum.map(join.joins, fn parent_hash ->
          edges = Map.get(facts_by_branch, parent_hash, [])

          edges
          |> Enum.sort_by(fn edge ->
            # Sort by item_index for correct pairing
            # Use the fact's item_index, tracing back through ancestry if needed
            get_fact_item_index(workflow, edge.v1)
          end)
          |> Enum.map(& &1.v1.value)
        end)

      # Apply the join mode to combine branch values
      joined_values = Join.apply_mode(join.mode, branch_values)

      # Produce a fact for each joined value
      produce_fan_out_join_results(join, workflow, fact, joined_values, causal_generation)
    else
      workflow
    end
  end

  defp check_all_branches_ready(join, workflow, facts_by_branch) do
    # For each branch, check if we have received all items from the fan-out
    Enum.all?(join.joins, fn parent_hash ->
      branch_facts = Map.get(facts_by_branch, parent_hash, [])

      if Enum.empty?(branch_facts) do
        false
      else
        # Check if this branch's fan-out has completed
        fan_out_hash = Map.get(join.fan_out_sources || %{}, parent_hash)

        if fan_out_hash do
          check_fan_out_complete(workflow, fan_out_hash, parent_hash, branch_facts)
        else
          # Not a fan-out branch, just need one fact
          true
        end
      end
    end)
  end

  defp check_fan_out_complete(workflow, fan_out_hash, parent_hash, branch_facts) do
    # Get all facts produced by this fan-out in the current generation
    fan_out_facts =
      workflow.graph
      |> Graph.out_edges(fan_out_hash, by: :fan_out)
      |> Enum.filter(fn edge ->
        fact_generation = get_fact_generation(workflow, edge.v2)
        fact_generation == workflow.generations
      end)
      |> length()

    # For the join to be ready, we need the same number of facts from this branch
    # as were produced by the fan-out (accounting for potential filtering in between)
    #
    # A more robust check: look at the mapped paths to see if all items have flowed through
    mapped_key = {workflow.generations, parent_hash}
    sister_fact_count = workflow.mapped[mapped_key] |> List.wrap() |> length()

    # The branch is ready if:
    # 1. We have at least one fact, AND
    # 2. Either the fan-out count matches our branch count, OR
    # 3. All items that passed through the parent step have reached the join
    branch_count = length(branch_facts)

    cond do
      # If we're tracking via mapped paths, use that
      sister_fact_count > 0 -> branch_count >= sister_fact_count
      # Otherwise, compare against fan-out production
      fan_out_facts > 0 -> branch_count >= fan_out_facts
      # Fallback: we have at least one fact
      true -> branch_count > 0
    end
  end

  defp get_fact_generation(workflow, fact) do
    workflow.graph
    |> Graph.in_edges(fact)
    |> Enum.find_value(fn edge ->
      if edge.label == :generation, do: edge.v1, else: nil
    end)
  end

  # Get the item index for a fact, tracing back through ancestry if needed
  defp get_fact_item_index(workflow, %Fact{item_index: index}) when not is_nil(index) do
    index
  end

  defp get_fact_item_index(workflow, %Fact{ancestry: {parent_hash, _}} = fact) do
    # Trace back through the ancestry to find the original fan-out item index
    parent_fact = find_ancestor_with_index(workflow, fact)

    case parent_fact do
      %Fact{item_index: index} when not is_nil(index) -> index
      # Fallback to hash for deterministic ordering
      _ -> fact.hash
    end
  end

  defp get_fact_item_index(_workflow, _fact), do: 0

  # Find the nearest ancestor fact that has an item_index
  defp find_ancestor_with_index(workflow, %Fact{item_index: index}) when not is_nil(index) do
    %Fact{item_index: index}
  end

  defp find_ancestor_with_index(workflow, %Fact{ancestry: nil}), do: nil

  defp find_ancestor_with_index(workflow, %Fact{ancestry: {_parent_step_hash, parent_fact_hash}}) do
    case Map.get(workflow.graph.vertices, parent_fact_hash) do
      %Fact{} = parent_fact -> find_ancestor_with_index(workflow, parent_fact)
      _ -> nil
    end
  end

  defp find_ancestor_with_index(_workflow, _), do: nil

  defp produce_join_result(join, workflow, fact, values, causal_generation) do
    join_bindings_fact = Fact.new(value: values, ancestry: {join.hash, fact.hash})

    workflow =
      workflow
      |> Workflow.log_fact(join_bindings_fact)
      |> Workflow.prepare_next_runnables(join, join_bindings_fact)

    workflow =
      workflow.graph
      |> Graph.in_edges(join)
      |> Enum.filter(&(&1.label in [:runnable, :joined]))
      |> Enum.reduce(workflow, fn
        %{v1: v1, label: :runnable}, wrk ->
          Workflow.mark_runnable_as_ran(wrk, join, v1)

        %{v1: v1, v2: v2, label: :joined}, wrk ->
          %Workflow{
            wrk
            | graph:
                wrk.graph |> Graph.update_labelled_edge(v1, v2, :joined, label: :join_satisfied)
          }
      end)

    workflow
    |> Workflow.draw_connection(join, join_bindings_fact, :produced, weight: causal_generation)
    |> Workflow.run_after_hooks(join, join_bindings_fact)
  end

  defp produce_fan_out_join_results(join, workflow, fact, joined_values, causal_generation) do
    # Calculate items_total for the joined output
    items_total = length(joined_values)

    {workflow, produced_facts} =
      joined_values
      |> Enum.with_index()
      |> Enum.reduce({workflow, []}, fn {values, index}, {wrk, facts} ->
        # Set item_index and items_total on joined facts
        # This ensures downstream steps have correct fan-out context
        joined_fact = Fact.new(
          value: values,
          ancestry: {join.hash, fact.hash},
          item_index: index,
          items_total: items_total
        )

        wrk =
          wrk
          |> Workflow.log_fact(joined_fact)
          |> Workflow.draw_connection(join, joined_fact, :produced, weight: causal_generation)

        {wrk, [joined_fact | facts]}
      end)

    # Mark all joined edges as satisfied
    workflow =
      workflow.graph
      |> Graph.in_edges(join)
      |> Enum.filter(&(&1.label in [:runnable, :joined]))
      |> Enum.reduce(workflow, fn
        %{v1: v1, label: :runnable}, wrk ->
          Workflow.mark_runnable_as_ran(wrk, join, v1)

        %{v1: v1, v2: v2, label: :joined}, wrk ->
          %Workflow{
            wrk
            | graph:
                wrk.graph |> Graph.update_labelled_edge(v1, v2, :joined, label: :join_satisfied)
          }
      end)

    # Prepare next runnables for each produced fact
    workflow =
      Enum.reduce(produced_facts, workflow, fn joined_fact, wrk ->
        wrk
        |> Workflow.prepare_next_runnables(join, joined_fact)
        |> Workflow.run_after_hooks(join, joined_fact)
      end)

    # Track in mapped paths if we're in a fan-out context
    if Join.fan_out_aware?(join) do
      key = {workflow.generations, join.hash}
      fact_hashes = Enum.map(produced_facts, & &1.hash)
      existing = workflow.mapped[key] || []

      %Workflow{
        workflow
        | mapped: Map.put(workflow.mapped, key, fact_hashes ++ existing)
      }
    else
      workflow
    end
  end

  def match_or_execute(_join), do: :execute
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.FanOut do
  alias Runic.Workflow
  alias Runic.Workflow.{Fact, FanOut, Components}

  def invoke(
        %FanOut{} = fan_out,
        %Workflow{} = workflow,
        %Fact{} = source_fact
      ) do
    items =
      if fan_out.work do
        Components.run(fan_out.work, source_fact.value, Components.arity_of(fan_out.work))
      else
        source_fact.value
      end

    items_list = if is_list(items), do: items, else: Enum.to_list(items)
    items_total = length(items_list)

    items_fact = Fact.new(value: items_list, ancestry: {fan_out.hash, source_fact.hash})

    unless is_nil(Enumerable.impl_for(items)) do
      causal_generation = Workflow.causal_generation(workflow, source_fact)
      is_reduced? = is_reduced?(workflow, fan_out)

      {workflow, _} =
        items_list
        |> Enum.with_index()
        |> Enum.reduce({workflow, 0}, fn {value, index}, {wrk, _idx} ->
          # Create fact with item index tracking
          fact =
            Fact.new(
              value: value,
              ancestry: {fan_out.hash, source_fact.hash},
              item_index: index,
              items_total: items_total,
              fan_out_hash: fan_out.hash
            )

          wrk =
            wrk
            |> Workflow.log_fact(fact)
            |> Workflow.prepare_next_runnables(fan_out, fact)
            |> Workflow.draw_connection(fan_out, fact, :fan_out, weight: causal_generation)
            |> maybe_prepare_map_reduce(is_reduced?, fan_out, fact)

          {wrk, index + 1}
        end)

      workflow
      |> Workflow.mark_runnable_as_ran(fan_out, source_fact)
      |> Workflow.run_after_hooks(fan_out, items_fact)
    else
      workflow
      |> Workflow.mark_runnable_as_ran(fan_out, source_fact)
      |> Workflow.run_after_hooks(fan_out, items_fact)
    end
  end

  defp maybe_prepare_map_reduce(workflow, true, fan_out, fan_out_fact) do
    key = {workflow.generations, fan_out.hash}
    sister_facts = workflow.mapped[key] || []

    Map.put(workflow, :mapped, Map.put(workflow.mapped, key, [fan_out_fact.hash | sister_facts]))
  end

  defp maybe_prepare_map_reduce(workflow, false, _fan_out, _fan_out_fact) do
    workflow
  end

  def is_reduced?(workflow, fan_out) do
    Graph.out_edges(workflow.graph, fan_out) |> Enum.any?(&(&1.label == :fan_in))
  end

  def match_or_execute(_fan_out), do: :execute
end

defimpl Runic.Workflow.Invokable, for: Runic.Workflow.FanIn do
  alias Runic.Workflow.FanOut
  alias Runic.Workflow

  alias Runic.Workflow.{
    Fact,
    FanIn
  }

  @spec invoke(%Runic.Workflow.FanIn{}, Runic.Workflow.t(), Runic.Workflow.Fact.t()) ::
          Runic.Workflow.t()
  def invoke(
        %FanIn{} = fan_in,
        %Workflow{} = workflow,
        %Fact{ancestry: {parent_step_hash, _parent_fact_hash}} = fact
      ) do
    fan_out =
      workflow.graph
      |> Graph.in_edges(fan_in)
      |> Enum.filter(&(&1.label == :fan_in))
      |> List.first(%{})
      |> Map.get(:v1)

    causal_generation = Workflow.causal_generation(workflow, fact)

    case fan_out do
      nil ->
        # basic step w/ enummerable output -> fan_in
        reduced_value = Enum.reduce(fact.value, fan_in.init.(), fan_in.reducer)

        reduced_fact =
          Fact.new(value: reduced_value, ancestry: {fan_in.hash, fact.hash})

        workflow
        |> Workflow.log_fact(reduced_fact)
        |> Workflow.run_after_hooks(fan_in, reduced_fact)
        |> Workflow.prepare_next_runnables(fan_in, reduced_fact)
        |> Workflow.draw_connection(fan_in, reduced_fact, :reduced, weight: causal_generation)
        |> Workflow.mark_runnable_as_ran(fan_in, fact)

      %FanOut{} ->
        # we may want to check ancestry paths from fan_out to fan_in with set inclusions

        sister_facts =
          for hash <- workflow.mapped[{workflow.generations, parent_step_hash}] || [] do
            workflow.graph.vertices
            |> Map.get(hash)
          end

        fan_out_facts_for_generation =
          workflow.graph
          |> Graph.out_edges(fan_out)
          |> Enum.filter(fn edge ->
            fact_generation =
              workflow.graph
              |> Graph.in_edges(edge.v2)
              |> Enum.filter(&(&1.label == :generation))
              |> List.first(%{})
              |> Map.get(:v1)

            edge.label == :fan_out and fact_generation == workflow.generations
          end)
          |> Enum.map(& &1.v2)
          |> Enum.uniq()

        # is a count safe or should we check set inclusions of hashes?
        if Enum.count(sister_facts) == Enum.count(fan_out_facts_for_generation) and
             not Enum.empty?(sister_facts) do
          sister_fact_values = Enum.map(sister_facts, & &1.value)

          reduced_value = Enum.reduce(sister_fact_values, fan_in.init.(), fan_in.reducer)

          fact =
            Fact.new(value: reduced_value, ancestry: {fan_in.hash, fact.hash})

          workflow =
            Enum.reduce(sister_facts, workflow, fn sister_fact, wrk ->
              wrk
              |> Workflow.mark_runnable_as_ran(fan_in, sister_fact)
            end)

          workflow
          |> Workflow.log_fact(fact)
          |> Workflow.run_after_hooks(fan_in, fact)
          |> Workflow.prepare_next_runnables(fan_in, fact)
          |> Workflow.draw_connection(fan_in, fact, :reduced, weight: causal_generation)
          |> Workflow.mark_runnable_as_ran(fan_in, fact)
          |> Map.put(
            :mapped,
            Map.delete(workflow.mapped, {workflow.generations, fan_out.hash})
          )
        else
          workflow
          |> Workflow.mark_runnable_as_ran(fan_in, fact)
        end
    end

    # if we have all facts needed then reduce otherwise mark as ran and let last sister fact reduce
  end

  def match_or_execute(_fan_in), do: :execute
end
