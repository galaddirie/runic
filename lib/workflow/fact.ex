defmodule Runic.Workflow.Fact do
  alias Runic.Workflow.Components

  defstruct [:hash, :value, :ancestry, :item_index, :items_total, :fan_out_hash]

  @type hash() :: integer() | binary()

  @type t() :: %__MODULE__{
          value: term(),
          hash: hash(),
          ancestry: {hash(), hash()} | nil,
          item_index: non_neg_integer() | nil,
          items_total: non_neg_integer() | nil,
          fan_out_hash: hash() | nil
        }

  def new(params) do
    struct!(__MODULE__, params)
    |> maybe_set_hash()
  end

  defp maybe_set_hash(%__MODULE__{value: value, hash: nil} = fact) do
    # Include item_index in hash to ensure uniqueness for fan-out items
    hash_input = {value, fact.ancestry, fact.item_index}
    %__MODULE__{fact | hash: Components.fact_hash(hash_input)}
  end

  defp maybe_set_hash(%__MODULE__{hash: hash} = fact)
       when not is_nil(hash),
       do: fact

  @doc """
  Creates a fan-out item fact with proper index tracking.
  """
  def new_fan_out_item(value, fan_out_hash, parent_fact_hash, index, total) do
    new(
      value: value,
      ancestry: {fan_out_hash, parent_fact_hash},
      item_index: index,
      items_total: total,
      fan_out_hash: fan_out_hash
    )
  end
end
