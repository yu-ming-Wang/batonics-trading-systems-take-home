## Order Book Update Logic & Assumptions

### Order Book Model

The engine reconstructs a **price-time priority limit order book** from MBO (Market-By-Order) events.

Internally, the order book is represented as:

* **Price-level maps**

  * `bids`: price → FIFO queue (highest price first)
  * `asks`: price → FIFO queue (lowest price first)
* **Per-order index**

  * `order_id → { side, price, iterator }`
  * Enables **O(1)** access for cancel and modify operations

Each price level maintains a FIFO queue to preserve **time priority** within the same price.

---

### Supported Event Types

The engine processes the following MBO actions:

* **A (Add)** – Insert a new resting order
* **C (Cancel)** – Reduce or remove an existing resting order
* **M (Modify)** – Change price and/or size of an existing order
* **R (Reset)** – Clear the entire order book

The following actions are **ignored** for book state changes:

* **T (Trade)**
* **F (Fill)**
* **N (None)**

This assumes that all resting book changes are reflected through subsequent `C` or `M` events.

---

### Update Rules

#### Add (`A`)

* A new order is appended to the **tail** of the FIFO queue at its price level.
* If an `order_id` already exists (defensive handling):

  * The existing order is removed from its previous price level.
  * The new order is inserted as a fresh order (loses priority).

#### Cancel (`C`)

* The order is located using `order_id`.
* Supports **partial cancel**:

  * If `cancel_size < current_size`: quantity is reduced in place.
  * If `cancel_size ≥ current_size`: order is fully removed.
* Empty price levels are removed automatically.

#### Modify (`M`)

* If the order does not exist, the event is treated as an **Add**.
* Modify rules follow standard price-time priority behavior:

  * **Price change** → order is removed from old level and appended to new level (loses priority)
  * **Size increase at same price** → order is re-appended to tail (loses priority)
  * **Size decrease or same size at same price** → quantity updated in place (keeps priority)

The `size` field in `M` events is treated as the **new absolute quantity**, not a delta.

#### Reset (`R`)

* Clears all bids, asks, and order index.
* Used to resynchronize book state.

---

### Assumptions

* Each `order_id` uniquely identifies a resting order at any time.
* Modify events (`M`) carry **absolute size**, while cancel events (`C`) carry **delta size**.
* Trade / fill events do not directly mutate the order book.
* Price-time priority is enforced strictly within each price level.
* The book is reconstructed **single-threaded** to guarantee deterministic ordering.
* Downstream consumers (WebSocket, DB, file output) never block the update path.

---

### Complexity Characteristics

* **Add / Cancel / Modify**:

  * O(1) at the order level (via `order_id` index)
  * O(log N) at the price level (map lookup)
* **Best Bid / Ask**: O(1)
* **Snapshot aggregation**: O(depth)

---

### Design Rationale

This design prioritizes:

* **Correctness** over speculative matching logic
* **Low and predictable latency** on the hot path
* **Clear separation** between reconstruction and downstream I/O
* **Defensive handling** of imperfect or duplicated input data

The goal is not to emulate a full exchange matching engine, but to provide a **fast, deterministic, and inspectable reconstruction** suitable for research, replay, and visualization.
