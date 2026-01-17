defmodule Runic.Workflow.Condition do
  alias Runic.Workflow.Components
  defstruct [:hash, :work, :arity, :name]

  def new(work) when is_function(work) do
    new(work, Function.info(work, :arity) |> elem(1))
  end

  def new(opts) do
    params_map = if Keyword.keyword?(opts), do: Map.new(opts), else: opts

    struct!(__MODULE__, params_map)
    |> maybe_set_name()
  end

  def new(work, arity) when is_function(work) do
    %__MODULE__{
      work: work,
      hash: Runic.Workflow.Components.work_hash(work),
      arity: arity
    }
    |> maybe_set_name()
  end

  defp maybe_set_name(%__MODULE__{name: nil, hash: hash} = s),
    do: %__MODULE__{s | name: to_string(hash)}

  defp maybe_set_name(%__MODULE__{name: name} = s) when not is_nil(name), do: s
end
