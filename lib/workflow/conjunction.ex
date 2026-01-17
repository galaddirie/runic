defmodule Runic.Workflow.Conjunction do
  alias Runic.Workflow.Components
  defstruct [:hash, :condition_hashes, :name]

  def new(params) do
    params_map = if Keyword.keyword?(params), do: Map.new(params), else: params

    struct!(__MODULE__, params_map)
    |> maybe_set_name()
  end

  defp maybe_set_name(%__MODULE__{name: nil, hash: hash} = s),
    do: %__MODULE__{s | name: to_string(hash)}

  defp maybe_set_name(%__MODULE__{name: name} = s) when not is_nil(name), do: s

  def new(conditions) do
    condition_hashes = conditions |> MapSet.new(& &1.hash)

    %__MODULE__{
      hash: condition_hashes |> Components.fact_hash(),
      condition_hashes: condition_hashes
    }
  end
end
