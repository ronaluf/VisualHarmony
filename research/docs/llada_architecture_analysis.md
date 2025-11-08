# LLaDA (Large Language Diffusion with mAsking) Architecture Analysis

## Executive Summary

**LLaDA** is a **diffusion-based language model** trained from scratch that rivals autoregressive models like LLaMA3 8B. Instead of predicting tokens left-to-right, LLaDA uses **masked diffusion**: iteratively unmasking tokens through a learned reverse process.

**Key Innovation:**
- Masked Diffusion Model (MDM) for discrete text
- Transformer Encoder (bidirectional attention, no causal mask)
- Pre-trained on 2.3T tokens, scaled to 8B parameters
- Competitive with LLaMA3 8B on benchmarks

**Key Advantages:**
- Addresses "reversal curse" (better bidirectional reasoning)
- In-context learning without autoregressive assumptions
- Flexible generation order (can unmask in any order)

**Key Challenges:**
- Slower sampling (256-1024 diffusion steps)
- Fixed context length during generation
- Cannot use KV-cache optimization

---

## 1. Core Concept

### The Big Idea

**Autoregressive (GPT/LLaMA):**
```
P(x₁, x₂, ..., xₙ) = P(x₁) · P(x₂|x₁) · P(x₃|x₁,x₂) · ...
Generate: x₁ → x₂ → x₃ → ...
```

**Masked Diffusion (LLaDA):**
```
Forward:  x₀ → [partially masked x_t] → [fully masked x₁]
Reverse:  [fully masked] → [partially masked] → x₀
Generate: Iteratively predict and unmask tokens
```

**Key Difference:**
- Autoregressive: Sequential, causal, fixed order
- Diffusion: Parallel, bidirectional, flexible order

---

## 2. Mathematical Formulation

### 2.1 Forward Process (Data → Noise)

Start with clean text `x₀`, gradually mask tokens:

```
Sample masking ratio: t ~ Uniform(0, 1)
Masking probability: p_mask = (1 - ε)·t + ε  (ε=0.01 for stability)

For each token:
  if rand() < p_mask:
    x_t[i] = [MASK]  (token ID 126336)
  else:
    x_t[i] = x₀[i]
```

**Key Property:** At t=0, no masking (clean text). At t=1, almost fully masked.

**Distribution:**
```
q(x_t | x₀) = ∏ᵢ [p_mask · δ(xₜᵢ = [MASK]) + (1-p_mask) · δ(xₜᵢ = x₀ᵢ)]
```

### 2.2 Reverse Process (Noise → Data)

Train a **mask predictor** `p_θ(x₀ | x_t)` to predict original tokens from masked input:

```
Input: x_t (partially masked sequence)
Output: logits over vocab for ALL positions
Loss: Cross-entropy on masked positions only
```

**Sampling:**
1. Start with fully masked sequence
2. For t = 1.0 → 0.0 (discretized steps):
   - Predict all tokens: `x₀_pred = argmax p_θ(x₀ | x_t)`
   - Unmask some tokens
   - Remask some tokens (for next iteration)
3. Return final sequence

### 2.3 Training Objective

The core loss function:

```python
def forward_process(input_ids, eps=1e-3):
    b, l = input_ids.shape
    t = torch.rand(b, device=input_ids.device)  # Random timestep per batch
    p_mask = (1 - eps) * t + eps
    p_mask = p_mask[:, None].repeat(1, l)

    masked_indices = torch.rand((b, l)) < p_mask
    noisy_batch = torch.where(masked_indices, 126336, input_ids)  # 126336 = [MASK]
    return noisy_batch, masked_indices, p_mask

# Training
noisy_batch, masked_indices, p_mask = forward_process(input_ids)
logits = model(input_ids=noisy_batch).logits

# Loss ONLY on masked tokens, weighted by inverse mask probability
token_loss = F.cross_entropy(
    logits[masked_indices],
    input_ids[masked_indices],
    reduction='none'
) / p_mask[masked_indices]

loss = token_loss.sum() / (input_ids.shape[0] * input_ids.shape[1])
```

**Key Details:**
1. **Variable masking ratio:** t sampled from U(0,1), not fixed like BERT
2. **Weighted loss:** Divide by `p_mask` to unbias the objective
3. **Only masked tokens:** Don't predict unmasked tokens (they're already known!)
4. **Random length:** 1% of data uses random length 1-4096 for robustness

**Theoretical Foundation:**
This objective is an **upper bound on negative log-likelihood** of the data distribution. This makes LLaDA a proper generative model (unlike BERT which is discriminative).

---

## 3. Model Architecture

### 3.1 Transformer Encoder (Not Decoder!)

**Key Difference from Autoregressive Models:**

```
Autoregressive (GPT):          LLaDA:
┌─────────────────┐           ┌─────────────────┐
│  Output Embed   │           │  Output Embed   │
│        ↑        │           │        ↑        │
│   Decoder Layer │           │  Encoder Layer  │
│   WITH causal   │           │   NO causal     │
│   mask (tril)   │           │   mask (full)   │
│        ↑        │           │        ↑        │
│  Input Embed    │           │  Input Embed    │
└─────────────────┘           └─────────────────┘
```

**From `GUIDELINES.md`:**
> "Starting from an autoregressive model, we derive the backbone of LLaDA by simply removing the causal mask from the self-attention mechanism"

**Implementation:**
```python
# Autoregressive attention
attn_weights = torch.softmax(Q @ K.T + causal_mask, dim=-1)
# where causal_mask = [[0, -inf, -inf],
#                      [0,    0, -inf],
#                      [0,    0,    0]]

# LLaDA attention (no mask!)
attn_weights = torch.softmax(Q @ K.T, dim=-1)
# All tokens can attend to all tokens
```

**Implications:**
- ✅ Bidirectional context (better for some tasks)
- ✅ Simpler architecture
- ❌ Cannot use autoregressive generation
- ❌ Must use iterative diffusion sampling

### 3.2 Architecture Specs

Based on README and Hugging Face models:

**LLaDA-8B-Base:**
- Parameters: ~8B (similar to LLaMA3 8B)
- Layers: (likely 32, standard for 8B models)
- Hidden size: (likely 4096)
- Attention heads: (likely 32)
- Vocab size: 126337 (includes [MASK] token at 126336)
- Context length: 4096 tokens (training)
- Pre-training tokens: 2.3 trillion

**Modifications from Standard Transformer:**
1. Remove causal mask
2. Add [MASK] token to vocabulary
3. Output logits for ALL positions (not just next token)

---

## 4. Supervised Fine-Tuning (SFT)

### 4.1 Data Format

LLaDA uses standard instruction-following format:

```
<BOS><start_id>user<end_id>
What is the capital of France?<eot_id>
<start_id>assistant<end_id>
Paris.<EOS><EOS>...
```

**Key Addition:** Track `prompt_lengths` to distinguish prompt from answer

### 4.2 SFT Training

**Crucial Difference from Pre-training:** Do NOT mask the prompt!

```python
input_ids, prompt_lengths = batch["input_ids"], batch["prompt_lengths"]

# Apply masking to full sequence
noisy_batch, _, p_mask = forward_process(input_ids)

# Restore prompt (no masking!)
token_positions = torch.arange(noisy_batch.shape[1]).expand(...)
prompt_mask = (token_positions < prompt_length.unsqueeze(1))
noisy_batch[prompt_mask] = input_ids[prompt_mask]  # Unmask prompt

# Compute loss only on answer tokens
masked_indices = (noisy_batch == 126336)
logits = model(input_ids=noisy_batch).logits

token_loss = F.cross_entropy(
    logits[masked_indices],
    input_ids[masked_indices],
    reduction='none'
) / p_mask[masked_indices]

# Weight by answer length for proper normalization
answer_lengths = torch.sum((1 - prompt_mask), dim=-1, keepdim=True)
ce_loss = torch.sum(token_loss / answer_lengths[masked_indices]) / batch_size
```

**Interpretation:**
- Given full prompt + partially masked answer
- Predict masked answer tokens
- Do NOT predict prompt tokens (conditional generation)

---

## 5. Inference / Sampling

### 5.1 Sampling Algorithm

**Autoregressive is simple:**
```python
for i in range(max_length):
    logits = model(x[:i])
    x[i] = sample(logits[-1])
```

**LLaDA is iterative:**
```python
# Initialize: fully masked
x = [MASK] * length

# Diffusion steps
for t in reverse(linspace(0, 1, num_steps)):  # e.g., 256 steps
    # Predict all tokens
    logits = model(x)
    x_pred = argmax(logits, dim=-1)

    # Unmask some tokens based on confidence or schedule
    # (Various strategies: semi-autoregressive, block-wise, etc.)
    mask_ratio = compute_mask_ratio(t)
    x = selective_unmask(x, x_pred, mask_ratio)
```

**Key Parameters:**
- `num_steps`: Number of diffusion steps (256-1024 for best quality)
- `block_size`: For semi-autoregressive (generate in chunks)
- `remasking_strategy`: How to decide which tokens to keep

### 5.2 Remasking Strategies

**From README FAQ:**

> "The mask predictor has successfully predicted the reasoning process. However, during the remasking process, the reasoning steps are masked out again."

**Example strategies:**
1. **Confidence-based:** Keep high-confidence predictions, remask low-confidence
2. **Block-based:** Unmask fixed-size blocks (e.g., 16 tokens at a time)
3. **Semi-autoregressive:** Generate left-to-right but in chunks

**Example from evaluation scripts:**
- `length=512, block=32`: Generate 512 tokens, 32 at a time
- `length=256, block=256`: Generate all 256 tokens in one shot
- `confidence`: Use model confidence to decide unmasking

---

## 6. Performance Characteristics

### 6.1 Quality

**From paper/README:**
- LLaDA-8B matches LLaMA3-8B on many benchmarks
- Better at "reversal tasks" (e.g., reversing poems) due to bidirectional attention
- Strong in-context learning (competitive with GPT-4o on some tasks)
- Scalability: Follows power law with compute/data

### 6.2 Efficiency

**Advantages:**
- Fixed context length during generation
- Fully parallelizable within each diffusion step

**Disadvantages:**
- ❌ Slower sampling: Need 256-1024 forward passes vs 1 per token
- ❌ Cannot use KV-cache (no sequential generation)
- ❌ Fixed context (e.g., always use 4096 even for short generation)

**From FAQ:**
> "Currently, LLaDA's sampling speed is slower than the autoregressive baseline for three reasons:
> 1. LLaDA samples with a fixed context length;
> 2. LLaDA cannot yet leverage techniques like KV-Cache;
> 3. LLaDA achieves optimal performance when the number of sampling steps equals the response length."

**Optimization Directions:**
- Block diffusion (reduce fixed context issue)
- Consistency distillation (reduce steps from 256 → 4-8)
- Cache methods (adapted from image diffusion)

**Potential speedup:** 100-1000x possible (like image diffusion evolution)

---

## 7. Complexity Analysis

### 7.1 Training Complexity

**Per forward pass:**
- Same as Transformer Encoder: O(L²d + Ld²)
- No causal mask, but attention is still O(L²)

**Compared to autoregressive training:**
- Similar or slightly faster (no need for causal masking infrastructure)
- Slightly more complex data preparation (masking)

### 7.2 Inference Complexity

**Per diffusion step:**
- Full Transformer forward: O(L²d + Ld²)
- **Total for generation:** O(T · L²d) where T = num_diffusion_steps

**Compared to autoregressive inference:**
```
Autoregressive: O(L²d) for full sequence
LLaDA: O(T · L²d) where T=256-1024
Slowdown: 256-1024x
```

**Critical Bottleneck:** This is where TTT integration could help massively!

---

## 8. Relationship to Other Approaches

### 8.1 vs BERT

**Similarities:**
- Both use masking
- Both use Transformer Encoder

**Key Differences:**
- BERT: Fixed 15% masking ratio
- LLaDA: Variable 0-100% masking ratio
- BERT: Discriminative (no proper generative model)
- LLaDA: Generative (upper bound on NLL)
- BERT: No iterative sampling
- LLaDA: Diffusion-based sampling

### 8.2 vs MaskGIT (Image)

**MaskGIT** (image generation with masking):
- Iterative unmasking for image tokens
- Variable masking ratio during training

**LLaDA adapts to text:**
- Discrete vocabulary (not continuous)
- Longer sequences (4096 vs 256 image tokens)
- Language-specific challenges (coherence, grammar)

### 8.3 vs Autoregressive

**Advantages of LLaDA:**
- ✅ Bidirectional context (addresses reversal curse)
- ✅ Flexible generation order
- ✅ No exposure bias (doesn't depend on previous predictions)
- ✅ Theoretically cleaner (joint distribution)

**Disadvantages:**
- ❌ Much slower sampling
- ❌ Cannot use KV-cache
- ❌ Less mature ecosystem

---

## 9. Key Files and Code References

### 9.1 Repository Structure

```
LLaDA/
├── README.md          # Main documentation
├── GUIDELINES.md      # Training/SFT guidelines
├── EVAL.md           # Evaluation instructions
├── generate.py       # Generation script
├── eval_llada.py     # Evaluation script
├── chat.py           # Interactive chat
├── app.py            # Gradio demo
└── opencompass/      # Evaluation framework
```

### 9.2 Model Loading

```python
from transformers import AutoModel, AutoTokenizer

tokenizer = AutoTokenizer.from_pretrained(
    'GSAI-ML/LLaDA-8B-Base',
    trust_remote_code=True
)

model = AutoModel.from_pretrained(
    'GSAI-ML/LLaDA-8B-Base',
    trust_remote_code=True,
    torch_dtype=torch.bfloat16
)
```

**Note:** Model code is in Hugging Face repo, uses `trust_remote_code=True`

### 9.3 Core Functions

Based on README:
- `get_log_likelihood()`: Compute P(x) for evaluation
- `generate()`: Conditional generation with diffusion sampling

**Not open-sourced:**
- Full training framework
- Pre-training data
- (But guidelines provided)

---

## 10. Training Guidelines Summary

### 10.1 From Autoregressive to LLaDA

**Minimal changes needed:**

```python
# Autoregressive training
logits = model(input_ids=input_ids).logits
loss = F.cross_entropy(logits[:, :-1].reshape(-1, vocab_size),
                       input_ids[:, 1:].reshape(-1))

# ↓ ↓ ↓ Modify to LLaDA ↓ ↓ ↓

# LLaDA training
noisy_batch, masked_indices, p_mask = forward_process(input_ids)
logits = model(input_ids=noisy_batch).logits
token_loss = F.cross_entropy(logits[masked_indices],
                             input_ids[masked_indices],
                             reduction='none') / p_mask[masked_indices]
loss = token_loss.mean()
```

**Architecture change:**
```python
# In attention layer
# OLD: attn_weights = softmax(Q @ K.T + causal_mask)
# NEW: attn_weights = softmax(Q @ K.T)
```

### 10.2 Pre-training Details

- Sequence length: 4096
- Random length: 1% of data uses random 1-4096
- Masking: t ~ U(0.01, 1.0)
- [MASK] token: 126336
- Training tokens: 2.3T
- Crash: Once at 1.2T tokens (fixed by reducing LR)

### 10.3 SFT Details

- Do NOT mask prompt
- Mask only answer
- Weight loss by answer length
- Standard instruction format

---

## 11. Strengths

1. **Theoretical Foundation**
   - Proper generative model (unlike BERT)
   - Upper bound on NLL
   - Connection to any-order autoregressive

2. **Addresses Reversal Curse**
   - Bidirectional attention helps
   - Better at tasks requiring backward reasoning

3. **Competitive Performance**
   - Matches LLaMA3 8B on benchmarks
   - Strong in-context learning
   - Scales with compute/data

4. **Flexible Generation**
   - Can generate in any order
   - Can do infilling naturally
   - Semi-autoregressive strategies

---

## 12. Limitations

1. **Slow Sampling**
   - 256-1024 forward passes
   - Fixed context length
   - Cannot use KV-cache
   - **This is the biggest issue!**

2. **Less Mature**
   - Newer paradigm, less explored
   - Fewer optimizations known
   - Smaller community

3. **Training Complexity**
   - Need to learn masking strategy
   - Sampling strategies not standardized
   - More hyperparameters (num_steps, block_size, etc.)

4. **Fixed Context**
   - Always uses max length (e.g., 4096)
   - Wasteful for short generations
   - Memory intensive

---

## 13. Critical Questions for TTT Integration

1. **Complexity Reduction**
   - Current: O(T · L²d) where T=256
   - With TTT: O(T · Ld²) where T=256
   - If d=64, L=4096: **Massive speedup** (~64x per step, ~16,000x total!)

2. **Architectural Compatibility**
   - LLaDA uses standard Transformer Encoder
   - TTT is a drop-in replacement for attention
   - Should be **architecturally compatible**!

3. **Masking Compatibility**
   - LLaDA masks tokens with special ID
   - TTT reconstructs using (XV - XK)
   - Need to understand if these can be unified

4. **Diffusion + TTT Interaction**
   - At each diffusion step t, run TTT inner loop
   - TTT hidden state W could carry information across steps?
   - Or reset W for each diffusion step?

5. **Sampling Efficiency**
   - Main pain point is 256-1024 steps
   - If TTT speeds up each step by 64x
   - And we can reduce steps to 64 (via better modeling)
   - Total speedup: 64 × 4 = **256x**!

---

## 14. Comparison Matrix

| Aspect | Autoregressive | LLaDA | TTT-LLaDA (Proposed) |
|--------|---------------|-------|---------------------|
| **Complexity (Training)** | O(L²d) | O(L²d) | O(Ld²) |
| **Complexity (Inference)** | O(Ld) | O(T·L²d) | O(T·Ld²) |
| **Sampling Speed** | Fast | Slow (256x) | Medium (4-16x?) |
| **Context Length** | Dynamic | Fixed | Fixed |
| **Bidirectional** | ❌ | ✅ | ✅ |
| **KV Cache** | ✅ | ❌ | ❌ (but W cache) |
| **Reversal Curse** | ❌ | ✅ | ✅ |

---

## Conclusion

LLaDA is a **theoretically grounded** and **empirically strong** alternative to autoregressive language modeling. The masked diffusion approach enables **bidirectional reasoning** and addresses the **reversal curse**.

**Main Limitation:** Slow sampling due to iterative diffusion process and quadratic attention complexity.

**TTT Integration Opportunity:**
- ✅ Architectural compatibility (both use Transformer-style layers)
- ✅ Massive complexity reduction (O(T·L²d) → O(T·Ld²))
- ✅ Potential for quality improvements (better modeling per step)
- ⚠️ Need to carefully design integration
- ⚠️ Diffusion + TTT interaction needs analysis

**Next Steps:**
1. Analyze compatibility of masking and TTT reconstruction
2. Design integration approaches
3. Evaluate potential improvements
