# TTT + LLaDA Integration: Visual Diagrams

This document contains comprehensive Mermaid diagrams explaining the TTT + LLaDA integration.

---

## Diagram 1: Architecture Overview - Where TTT Integrates

```mermaid
graph TB
    subgraph "Standard LLaDA Architecture"
        A1[Input: Masked Text] --> B1[Embedding Layer]
        B1 --> C1[Transformer Encoder Layer 1]
        C1 --> D1[...]
        D1 --> E1[Transformer Encoder Layer N]
        E1 --> F1[LM Head]
        F1 --> G1[Output: Logits]

        subgraph "Transformer Encoder Layer"
            C1A[Input] --> C1B[Self-Attention<br/>O L² d complexity]
            C1B --> C1C[Add & Norm]
            C1C --> C1D[Feed-Forward]
            C1D --> C1E[Add & Norm]
            C1E --> C1F[Output]
        end
    end

    subgraph "TTT-LLaDA Architecture PROPOSED"
        A2[Input: Masked Text] --> B2[Embedding Layer]
        B2 --> C2[TTT Encoder Layer 1]
        C2 --> D2[...]
        D2 --> E2[TTT Encoder Layer N]
        E2 --> F2[LM Head]
        F2 --> G2[Output: Logits]

        subgraph "TTT Encoder Layer REPLACEMENT"
            C2A[Input] --> C2B[TTT Layer<br/>✓ O L d² complexity]
            C2B --> C2C[Add & Norm]
            C2C --> C2D[Feed-Forward]
            C2D --> C2E[Add & Norm]
            C2E --> C2F[Output]
        end
    end

    style C1B fill:#ffcccc
    style C2B fill:#ccffcc

    linkStyle default stroke:#333,stroke-width:2px
```

**Key Points:**
- 🔴 **Red Box:** Standard Self-Attention (O(L²d) - SLOW!)
- 🟢 **Green Box:** TTT Layer (O(Ld²) - FAST!)
- **Change:** Only attention layer is replaced, everything else stays the same!

---

## Diagram 2: TTT Layer Internal Structure

```mermaid
graph TB
    subgraph "TTT Layer Detailed View"
        Input[Input Sequence<br/>B, L, d_model] --> Proj[Q/K/V Projections]

        Proj --> Q[Query XQ<br/>B, L, d_model]
        Proj --> K[Key XK<br/>B, L, d_model]
        Proj --> V[Value XV<br/>B, L, d_model]

        Q --> Split1[Split into<br/>mini-batches]
        K --> Split2[Split into<br/>mini-batches]
        V --> Split3[Split into<br/>mini-batches]

        Split1 --> MB1[Mini-batch 1<br/>B, 16, d]
        Split2 --> MB2[Mini-batch 1<br/>B, 16, d]
        Split3 --> MB3[Mini-batch 1<br/>B, 16, d]

        MB1 & MB2 & MB3 --> TTT[TTT Inner Loop<br/>Self-supervised Learning]

        TTT --> W[Update Hidden State W<br/>W = W - η × gradient]
        W --> Output[Output<br/>B, 16, d_model]

        Output --> Concat[Concatenate<br/>all mini-batches]
        Concat --> Final[Final Output<br/>B, L, d_model]

        subgraph "Inner Loop Detail"
            TTT1[1. Compute: Z = XK @ W + b] --> TTT2[2. Target: t = XV - XK]
            TTT2 --> TTT3[3. Loss: ||Z - t||²]
            TTT3 --> TTT4[4. Gradient: ∂Loss/∂W]
            TTT4 --> TTT5[5. Update: W ← W - η × grad]
        end
    end

    style TTT fill:#ffffcc
    style W fill:#ffccff
```

**Key Points:**
- **Mini-batches:** Sequence split into chunks of 16 tokens
- **Inner Loop:** Self-supervised learning on each mini-batch
- **Hidden State W:** Learned model that carries information
- **Dual Form:** Efficient implementation using matrix operations

---

## Diagram 3: How TTT Helps - Complexity Comparison

```mermaid
graph LR
    subgraph "Standard Attention ONE Diffusion Step"
        A1[Masked Input<br/>L=4096 tokens] --> A2[Attention Layer 1<br/>Complexity: L² × d<br/>= 4096² × 4096<br/>= 68.7B ops]
        A2 --> A3[Attention Layer 2<br/>68.7B ops]
        A3 --> A4[...]
        A4 --> A5[Attention Layer 24<br/>68.7B ops]
        A5 --> A6[Total: 1.65T ops<br/>per diffusion step]
    end

    subgraph "TTT Architecture ONE Diffusion Step"
        B1[Masked Input<br/>L=4096 tokens] --> B2[TTT Layer 1<br/>Complexity: L × d²<br/>= 4096 × 128²<br/>= 67M ops]
        B2 --> B3[TTT Layer 2<br/>67M ops]
        B3 --> B4[...]
        B4 --> B5[TTT Layer 24<br/>67M ops]
        B5 --> B6[Total: 1.6B ops<br/>per diffusion step]
    end

    A6 -.->|Speedup per step<br/>1.65T / 1.6B<br/>= 1000×| B6

    subgraph "Full Generation"
        C1[256 diffusion steps<br/>Standard Attention] --> C2[256 × 1.65T<br/>= 422T ops<br/>~5 minutes]

        D1[256 diffusion steps<br/>TTT] --> D2[256 × 1.6B<br/>= 410B ops<br/>~15 seconds]

        D3[128 steps possible<br/>with better model] --> D4[128 × 1.6B<br/>= 205B ops<br/>~8 seconds]
    end

    style A6 fill:#ffcccc
    style B6 fill:#ccffcc
    style C2 fill:#ffcccc
    style D2 fill:#ccffcc
    style D4 fill:#ccffff
```

**Key Improvements:**
1. **Per-step speedup:** ~1000× theoretical (32-64× practical)
2. **Fewer steps needed:** 256 → 128 (TTT enables better modeling)
3. **Total speedup:** ~10-20× for full generation

---

## Diagram 4: Training Process Flow

```mermaid
flowchart TD
    Start([Start Training]) --> Init[Initialize TTT-LLaDA Model<br/>350M params, random weights]

    Init --> LoadData[Load Training Data<br/>High-quality text: 5B tokens]

    LoadData --> Batch{For each batch}

    Batch -->|Get batch| Mask[Apply Random Masking<br/>t ~ U0.2, 0.8<br/>Mask t% of tokens]

    Mask --> Forward[Forward Pass through TTT-LLaDA]

    subgraph "Forward Pass Details"
        Forward1[Embed masked input] --> Forward2[Layer 1: TTT processes<br/>Inner loop trains W on this batch]
        Forward2 --> Forward3[Layer 2: TTT processes<br/>Uses W from layer 1]
        Forward3 --> Forward4[...]
        Forward4 --> Forward5[Layer 24: TTT processes]
        Forward5 --> Forward6[LM Head: Output logits]
    end

    Forward --> Loss[Compute Loss<br/>CrossEntropy on masked tokens<br/>weighted by 1/p_mask]

    Loss --> Backward[Backward Pass<br/>Compute gradients for:<br/>- Outer params θ<br/>- Through TTT inner loops]

    Backward --> Update[Update Outer Parameters<br/>θ ← θ - lr × ∇θ Loss]

    Update --> Check{More batches?}

    Check -->|Yes| Batch
    Check -->|No| Eval[Evaluate on Val Set<br/>Compute perplexity]

    Eval --> Done{Training complete?<br/>5B tokens seen}

    Done -->|No| Batch
    Done -->|Yes| Save[Save Model Checkpoint]

    Save --> End([Training Complete])

    style Forward fill:#ffffcc
    style Loss fill:#ffcccc
    style Update fill:#ccffcc
    style Eval fill:#ccccff
```

**Training Details:**
- **Batch size:** 8 per GPU (16 total on 2 GPUs)
- **Learning rate:** 3e-4 with cosine schedule
- **Gradient accumulation:** 4 steps (effective batch = 64)
- **Training time:** ~40 days on 2× RTX 6000
- **Checkpoints:** Every 100M tokens

---

## Diagram 5: Inference/Generation Process (Diffusion Sampling)

```mermaid
sequenceDiagram
    participant User
    participant Model as TTT-LLaDA
    participant State as TTT Hidden State W

    User->>Model: Prompt: "Explain quantum computing"

    Note over Model: Initialize fully masked sequence<br/>[M][M][M]...[M] (256 tokens)

    loop Diffusion Steps (t = 1.0 → 0.0)
        Note over Model,State: Diffusion Step t

        Model->>State: Reset W to initial values<br/>(fresh start each step)

        Model->>Model: Forward pass:<br/>Process masked sequence

        Note over Model: TTT Inner Loop:<br/>W learns from current sequence

        Model->>Model: Generate predictions<br/>for all positions

        Model->>Model: Select tokens to unmask<br/>based on confidence/schedule

        Note over Model: Unmask high-confidence tokens<br/>[M][M][quantum][M]...

        Note over Model: Remask some tokens randomly<br/>(for next iteration)
    end

    Note over Model: t = 0: Fully unmasked
    Model->>User: Output: "Quantum computing uses..."

    Note right of Model: Speed comparison:<br/>Standard: ~5 minutes<br/>TTT: ~30 seconds<br/>Speedup: 10×
```

**Inference Strategy:**
- **Reset TTT state:** Each diffusion step starts fresh (Approach 1)
- **Diffusion steps:** 128-256 (fewer than standard LLaDA)
- **Unmasking:** Progressive, high-confidence first
- **Speedup:** Each step is 32× faster → total ~10-15× faster

---

## Diagram 6: Why TTT Works - Hidden State Evolution

```mermaid
stateDiagram-v2
    [*] --> W_init: Initialize W randomly

    W_init --> W_mb1: Process mini-batch 1<br/>Learn patterns in first 16 tokens

    note right of W_mb1
        W learns:
        - Local syntax patterns
        - Token co-occurrences
        - Domain-specific features
    end note

    W_mb1 --> W_mb2: Process mini-batch 2<br/>Refine W on next 16 tokens

    W_mb2 --> W_mb3: Process mini-batch 3

    W_mb3 --> W_final: Process all mini-batches<br/>W now contains sequence info

    note right of W_final
        W encodes:
        - Sequence-specific patterns
        - Long-range dependencies
        - Compressed representation

        This helps predict masked tokens!
    end note

    W_final --> [*]: Use W for predictions
```

**How Hidden State Helps:**
1. **Learns sequence patterns:** W adapts to current input
2. **Compresses information:** Long sequence → compact W matrix
3. **Test-time adaptation:** Like few-shot learning, but automatic
4. **Better predictions:** Sequence-aware features improve masking prediction

---

## Diagram 7: Training Timeline (RTX 6000 Setup)

```mermaid
gantt
    title TTT-LLaDA Development Timeline (2× RTX 6000)
    dateFormat YYYY-MM-DD
    section Phase 1: PoC
    Implementation           :a1, 2025-12-01, 14d
    Quick Validation 125M    :a2, after a1, 10d
    Go/No-Go Decision       :milestone, after a2, 0d

    section Phase 2: Scaling
    Setup 350M Training     :b1, after a2, 7d
    Train 350M 1B tokens    :b2, after b1, 20d
    Train 350M 5B tokens    :b3, after b2, 40d

    section Phase 3: Eval
    Benchmarking            :c1, after b3, 14d
    Paper Writing           :c2, after c1, 14d
    Code Release Prep       :c3, after c1, 14d

    section Milestones
    Start                   :milestone, 2025-12-01, 0d
    PoC Complete           :milestone, after a2, 0d
    Training Complete      :milestone, after b3, 0d
    Submission             :milestone, after c2, 0d
```

**Total Duration:** ~4 months (120 days)
**Compute:** 2× RTX 6000 running continuously
**Cost:** $0 (owned hardware)

---

## Diagram 8: Memory Usage Comparison

```mermaid
graph TB
    subgraph "Standard Attention Memory 350M model"
        A1[Model Params: 1.4 GB] --> A2[Optimizer States: 2.8 GB]
        A2 --> A3[Gradients: 1.4 GB]
        A3 --> A4[Activations: 12 GB]
        A4 --> A5[Attention Matrices: 25.7 GB]
        A5 --> A6[Total: ~43 GB<br/>❌ Tight on 48GB GPU]
    end

    subgraph "TTT Memory 350M model"
        B1[Model Params: 1.4 GB] --> B2[Optimizer States: 2.8 GB]
        B2 --> B3[Gradients: 1.4 GB]
        B3 --> B4[Activations: 12 GB]
        B4 --> B5[TTT Hidden States: 0.1 GB]
        B5 --> B6[Total: ~18 GB<br/>✅ Comfortable on 48GB GPU]
    end

    A6 -.->|Memory Savings<br/>43GB → 18GB<br/>2.4× reduction| B6

    style A6 fill:#ffcccc
    style B6 fill:#ccffcc
```

**Benefits:**
- **2.4× less memory** → Can use larger batch sizes
- **Larger batches** → Faster training (better GPU utilization)
- **Room to spare** → Can experiment with larger models (500M-760M)

---

## Diagram 9: Approach Comparison Decision Tree

```mermaid
flowchart TD
    Start{Start Integration} --> Impl1[Implement Approach 1:<br/>Direct Replacement]

    Impl1 --> Train1[Train 125M on 1B tokens]

    Train1 --> Check1{Results?}

    Check1 -->|Training fails| Debug[Debug:<br/>- Check TTT implementation<br/>- Verify masking logic<br/>- Reduce learning rate]

    Debug --> Check1

    Check1 -->|Trains but quality -5%| Stop[❌ Stop:<br/>Fundamental incompatibility]

    Check1 -->|Quality -2% to +2%| Speedup{Speedup ≥ 3×?}

    Speedup -->|No| Optimize[Optimize:<br/>- Profile code<br/>- Improve kernels<br/>- Check implementation]

    Optimize --> Speedup

    Speedup -->|Yes| Scale1[✅ Scale to 350M<br/>Approach 1]

    Scale1 --> Publish1[Publish: Speedup focus<br/>Quality parity]

    Check1 -->|Quality +2% to +5%| Try2{Try Approach 2?}

    Try2 -->|No, good enough| Scale1
    Try2 -->|Yes| Impl2[Implement Approach 2:<br/>Unified Task]

    Impl2 --> Train2[Train 125M with unified loss]

    Train2 --> Check2{Better than Approach 1?}

    Check2 -->|No| Scale1
    Check2 -->|Yes +1-2%| Scale2[✅ Scale to 350M<br/>Approach 2]

    Scale2 --> Publish2[Publish: Quality + Speedup]

    style Stop fill:#ffcccc
    style Publish1 fill:#ccffcc
    style Publish2 fill:#ccffff
```

**Decision Points:**
1. **After 125M training:** Does it work at all? (Go/No-Go)
2. **Speedup check:** Is it actually faster? (Optimize or proceed)
3. **Quality check:** Worth trying Approach 2? (Optional enhancement)

---

## Diagram 10: Speedup Breakdown

```mermaid
pie title Computation Time Breakdown Standard LLaDA
    "Attention QK^T" : 40
    "Attention Softmax" : 10
    "Attention Value Multiply" : 30
    "FFN" : 15
    "Other Layer, Embedding" : 5

pie title Computation Time Breakdown TTT-LLaDA
    "TTT Inner Loop" : 35
    "TTT Dual Form" : 25
    "FFN (unchanged)" : 30
    "Other (Layer, Embedding)" : 10
```

**Analysis:**
- **Standard:** 80% time in attention (QK^T, softmax, multiply)
- **TTT:** 60% time in TTT (inner loop + dual form)
- **FFN:** Unchanged (30% vs 15% due to different total)
- **Net effect:** Attention 80% → TTT 60%, and TTT is faster per op
- **Result:** ~3-4× speedup overall

---

## Summary Diagram: The Big Picture

```mermaid
graph TB
    subgraph "Problem"
        P1[LLaDA is SLOW<br/>256 diffusion steps<br/>Quadratic attention O L²]
        P2[Each step takes 1 second<br/>Total: 256 seconds ~4 min]
    end

    subgraph "Solution: TTT Integration"
        S1[Replace Attention with TTT<br/>Linear complexity O L d²]
        S2[Reduce to 32× faster per step<br/>~0.03 seconds per step]
        S3[Enable fewer diffusion steps<br/>128 instead of 256]
    end

    subgraph "Result"
        R1[Each step: 0.03s × 128 steps<br/>= 3.84 seconds total]
        R2[Speedup: 256s / 3.84s<br/>= 67× faster! 🚀]
        R3[Quality: Similar or better<br/>Perplexity: ±2%]
    end

    subgraph "Implementation with RTX 6000"
        I1[Model: 350M params<br/>not 8B, but enough!]
        I2[Training: 1-5B tokens<br/>not 2.3T, but sufficient!]
        I3[Timeline: 4 months<br/>Cost: $0 owned hardware]
        I4[Publication: Workshop or arXiv<br/>Proof-of-concept valuable!]
    end

    P1 --> S1
    P2 --> S2
    S1 & S2 --> S3
    S3 --> R1
    R1 --> R2
    R2 --> R3
    R3 --> I1
    I1 --> I2
    I2 --> I3
    I3 --> I4

    style P1 fill:#ffcccc
    style P2 fill:#ffcccc
    style R2 fill:#ccffcc
    style R3 fill:#ccffcc
    style I4 fill:#ccffff
```

---

## How to Use These Diagrams

### For Understanding:
- **Start with Diagram 1:** See where TTT fits
- **Read Diagram 2:** Understand TTT internals
- **Study Diagram 3:** See why speedup happens

### For Implementation:
- **Diagram 4:** Follow training process
- **Diagram 7:** Plan your timeline
- **Diagram 9:** Make decisions as you progress

### For Presentation:
- **Diagram 10 & Summary:** Explain to others
- **Diagram 6:** Motivate why TTT helps
- **Diagram 8:** Show memory advantages

---

## Conclusion

These diagrams illustrate:

1. **Where:** TTT replaces attention layers (Diagram 1-2)
2. **How it helps:** O(L²) → O(Ld²) complexity (Diagram 3, 10)
3. **How to train:** Standard masked diffusion training (Diagram 4)
4. **What's feasible:** 350M model on RTX 6000 (Diagram 7-8)
5. **Expected outcome:** 10-67× speedup with quality parity (Summary)

**With 2× RTX 6000, you can absolutely do this research in 4 months!**
