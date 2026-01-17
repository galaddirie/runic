defmodule Runic.Workflow.Join do
  @moduledoc """
  Join steps combine facts from multiple parent branches into a single fact.

  ## Join Modes

  - `:wait_all` - (default for non-fan-out) Wait for one fact from each parent, combine into list
  - `:zip_nil` - (default for fan-out) Zip items by index, pad shorter branches with `nil`
  - `:zip_shortest` - Zip items by index, stop at shortest branch
  - `:zip_cycle` - Zip items by index, cycle shorter branches
  - `:cartesian` - Produce all combinations (use with caution)

  ## Fan-Out Awareness

  When `fan_out_sources` is populated, the join tracks items from each fan-out
  branch separately and applies the join mode to combine them by item index.
  """

  alias Runic.Workflow.Components

  @type join_mode :: :wait_all | :zip_nil | :zip_shortest | :zip_cycle | :cartesian

  @type t :: %__MODULE__{
          hash: integer(),
          name: String.t() | nil,
          joins: [integer()],
          mode: join_mode(),
          fan_out_sources: %{integer() => integer()} | nil
        }

  defstruct [:hash, :name, :joins, mode: :wait_all, fan_out_sources: nil]

  @doc """
  Creates a new Join from a list of parent step hashes.
  """
  def new(joins) when is_list(joins) do
    %__MODULE__{
      joins: joins,
      hash: Components.fact_hash(joins)
    }
  end

  @doc """
  Creates a new Join with options.

  ## Options

  - `:name` - Optional name for the join
  - `:mode` - Join mode (default: `:wait_all`)
  - `:fan_out_sources` - Map of parent_hash => fan_out_hash for fan-out aware joining
  """
  def new(joins, opts) when is_list(joins) and is_list(opts) do
    name = Keyword.get(opts, :name)
    mode = Keyword.get(opts, :mode, :wait_all)
    fan_out_sources = Keyword.get(opts, :fan_out_sources)

    # If we have fan_out_sources and mode is still :wait_all, default to :zip_nil
    mode =
      if fan_out_sources && map_size(fan_out_sources) > 0 && mode == :wait_all do
        :zip_nil
      else
        mode
      end

    %__MODULE__{
      joins: joins,
      hash: Components.fact_hash({joins, name, mode}),
      name: name,
      mode: mode,
      fan_out_sources: fan_out_sources
    }
  end

  @doc """
  Returns true if this join is fan-out aware.
  """
  def fan_out_aware?(%__MODULE__{fan_out_sources: nil}), do: false
  def fan_out_aware?(%__MODULE__{fan_out_sources: sources}) when map_size(sources) == 0, do: false
  def fan_out_aware?(%__MODULE__{}), do: true

  @doc """
  Applies the join mode to combine values from multiple branches.

  ## Parameters

  - `mode` - The join mode to apply
  - `branch_values` - List of lists, where each inner list contains values from one branch

  ## Returns

  A list of joined values (tuples or lists depending on mode).
  """
  def apply_mode(:wait_all, branch_values) do
    # Simple case: one value per branch, combine into single list
    [List.flatten(branch_values)]
  end

  def apply_mode(:zip_nil, branch_values) do
    max_len = branch_values |> Enum.map(&length/1) |> Enum.max(fn -> 0 end)

    if max_len == 0 do
      []
    else
      padded =
        Enum.map(branch_values, fn values ->
          values ++ List.duplicate(nil, max_len - length(values))
        end)

      0..(max_len - 1)
      |> Enum.map(fn i ->
        Enum.map(padded, &Enum.at(&1, i))
      end)
    end
  end

  def apply_mode(:zip_shortest, branch_values) do
    min_len = branch_values |> Enum.map(&length/1) |> Enum.min(fn -> 0 end)

    if min_len == 0 do
      []
    else
      0..(min_len - 1)
      |> Enum.map(fn i ->
        Enum.map(branch_values, &Enum.at(&1, i))
      end)
    end
  end

  def apply_mode(:zip_cycle, branch_values) do
    max_len = branch_values |> Enum.map(&length/1) |> Enum.max(fn -> 0 end)

    if max_len == 0 do
      []
    else
      0..(max_len - 1)
      |> Enum.map(fn i ->
        Enum.map(branch_values, fn values ->
          if length(values) == 0 do
            nil
          else
            Enum.at(values, rem(i, length(values)))
          end
        end)
      end)
    end
  end

  def apply_mode(:cartesian, branch_values) do
    case branch_values do
      [] ->
        []

      [single] ->
        Enum.map(single, &[&1])

      [first | rest] ->
        rest_cartesian = apply_mode(:cartesian, rest)

        for a <- first, b <- rest_cartesian do
          [a | b]
        end
    end
  end
end
