defmodule Runic.Workflow.MemoryAssertion do
  alias Runic.Workflow.Components
  defstruct [:memory_assertion, :hash, :state_hash, :name]

  def new(opts) do
    params_map = if Keyword.keyword?(opts), do: Map.new(opts), else: opts
    struct!(__MODULE__, params_map)
    |> maybe_set_name()
  end

  def new(memory_assertion, state_hash) do
    %__MODULE__{
      state_hash: state_hash,
      memory_assertion: memory_assertion,
      hash: Runic.Workflow.Components.work_hash(memory_assertion)
    }
    |> maybe_set_name()
  end

  defp maybe_set_name(%__MODULE__{name: nil, hash: hash} = s),
    do: %__MODULE__{s | name: to_string(hash)}

  defp maybe_set_name(%__MODULE__{name: name} = s) when not is_nil(name), do: s
end
