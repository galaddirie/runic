## invoke_with_events test (mix run test_runic_events.exs)

- Environment: ran `mix run test_runic_events.exs` in project root.
- Warnings: only unused-variable warnings in `lib/workflow.ex` (existing code; no run impact).

### Workflow under test
- Pipeline: `double -> add_ten -> format` starting with input `5`.

### Step results
- **double (hash 3003979339)**  
  - Input fact: hash 1209013588, ancestry: `nil`.  
  - `invoke_with_events` returned **1 event**:
    ```elixir
    %Runic.Workflow.ReactionOccurred{
      reaction: :produced,
      weight: 0,
      from: %Runic.Workflow.Step{hash: 3003979339},
      to: %Runic.Workflow.Fact{value: 10, hash: 1265825978}
    }
    ```

- **add_ten (hash 2881890656)**  
  - Input fact: hash 1265825978, ancestry: `{3003979339, 1209013588}`.  
  - `invoke_with_events` returned **1 event**:  
    ```elixir
    %Runic.Workflow.ReactionOccurred{
      reaction: :produced,
      weight: 1,
      from: %Runic.Workflow.Step{hash: 2881890656},
      to: %Runic.Workflow.Fact{value: 20, hash: 2030723375}
    }
    ```

- **format (hash 3243952311)**  
  - Input fact: hash 2030723375, ancestry: `{2881890656, 1265825978}`.  
  - `invoke_with_events` returned **1 event**:
    ```elixir
    %Runic.Workflow.ReactionOccurred{
      reaction: :produced,
      weight: 1,
      from: %Runic.Workflow.Step{hash: 3243952311},
      to: %Runic.Workflow.Fact{value: "Result: 20", hash: 1453960284}
    }
    ```

### Final productions
- `20`, `10`, `"Result: 20"`.

### Takeaway
- After the fix, `invoke_with_events` emits events for every step; no filtering gaps remain.

### Terminal output
```
Compiling 1 file (.ex)
     warning: variable "component_name" is unused (if the variable is not meant to be used, prefix it with an underscore)
     │
 896 │   def productions(%__MODULE__{} = wrk, component_name) do
     │                                        ~~~~~~~~~~~~~~
     │
     └─ lib/workflow.ex:896:40: Runic.Workflow.productions/2

     warning: variable "wrk" is unused (if the variable is not meant to be used, prefix it with an underscore)
     │
 896 │   def productions(%__MODULE__{} = wrk, component_name) do
     │                                   ~~~
     │
     └─ lib/workflow.ex:896:35: Runic.Workflow.productions/2

     warning: variable "components" is unused (if the variable is not meant to be used, prefix it with an underscore)
     │
 904 │   def productions_by_component(%__MODULE__{graph: graph, components: components}) do
     │                                                                      ~~~~~~~~~~
     │
     └─ lib/workflow.ex:904:70: Runic.Workflow.productions_by_component/1

     warning: variable "graph" is unused (if the variable is not meant to be used, prefix it with an underscore)
     │
 904 │   def productions_by_component(%__MODULE__{graph: graph, components: components}) do
     │                                                   ~~~~~
     │
     └─ lib/workflow.ex:904:51: Runic.Workflow.productions_by_component/1

     warning: variable "component_name" is unused (if the variable is not meant to be used, prefix it with an underscore)
     │
 925 │   def raw_productions(%__MODULE__{graph: graph}, component_name) do
     │                                                  ~~~~~~~~~~~~~~
     │
     └─ lib/workflow.ex:925:50: Runic.Workflow.raw_productions/2

     warning: variable "graph" is unused (if the variable is not meant to be used, prefix it with an underscore)
     │
 925 │   def raw_productions(%__MODULE__{graph: graph}, component_name) do
     │                                          ~~~~~
     │
     └─ lib/workflow.ex:925:42: Runic.Workflow.raw_productions/2

     warning: variable "graph" is unused (if the variable is not meant to be used, prefix it with an underscore)
     │
 928 │   def raw_productions_by_component(%__MODULE__{graph: graph}) do
     │                                                       ~~~~~
     │
     └─ lib/workflow.ex:928:55: Runic.Workflow.raw_productions_by_component/1

Generated runic app
================================================================================
Testing Runic invoke_with_events behavior
================================================================================

📋 Workflow components:
  - format: hash=3243952311
  - double: hash=3003979339
  - add_ten: hash=2881890656

📥 Planning input: 5

🔍 Initial runnables after planning:
  - Step: double (hash=3003979339)
    Input fact: value=5, hash=1209013588, ancestry=nil

================================================================================
Executing steps and examining invoke_with_events output
================================================================================

------------------------------------------------------------
Step 1: double
------------------------------------------------------------

  📥 Input:
    - Fact value: 5
    - Fact hash: 1209013588
    - Fact ancestry: nil

  🔧 Invoking with events...

  📤 Results:
    - Events count: 1

    Event 1:
      - Type: Runic.Workflow.ReactionOccurred
      - Reaction: :produced
      - Weight: 0
      - From: Runic.Workflow.Step (hash=3003979339)
      - To: Runic.Workflow.Fact
        → value: 10
        → hash: 1265825978

------------------------------------------------------------
Step 2: add_ten
------------------------------------------------------------

  📥 Input:
    - Fact value: 10
    - Fact hash: 1265825978
    - Fact ancestry: {3003979339, 1209013588}

  🔧 Invoking with events...

  📤 Results:
    - Events count: 1

    Event 1:
      - Type: Runic.Workflow.ReactionOccurred
      - Reaction: :produced
      - Weight: 1
      - From: Runic.Workflow.Step (hash=2881890656)
      - To: Runic.Workflow.Fact
        → value: 20
        → hash: 2030723375

------------------------------------------------------------
Step 3: format
------------------------------------------------------------

  📥 Input:
    - Fact value: 20
    - Fact hash: 2030723375
    - Fact ancestry: {2881890656, 1265825978}

  🔧 Invoking with events...

  📤 Results:
    - Events count: 1

    Event 1:
      - Type: Runic.Workflow.ReactionOccurred
      - Reaction: :produced
      - Weight: 1
      - From: Runic.Workflow.Step (hash=3243952311)
      - To: Runic.Workflow.Fact
        → value: "Result: 20"
        → hash: 1453960284

✅ No more runnables - workflow complete!

📊 Final productions:
  - 20
  - 10
  - "Result: 20"

================================================================================
Test complete!
================================================================================
```
