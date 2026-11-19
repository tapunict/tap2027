#!/usr/bin/env bash
# Export a Hugging Face model to ONNX and prepare it for Spark NLP loadSavedModel().
# Runs in a throw-away container so the Spark NLP image does not need PyTorch.
#
# Usage: ./export_onnx.sh [hf_model_id] [output_dir_name] [task]
set -euo pipefail

MODEL=${1:-distilbert/distilbert-base-uncased-finetuned-sst-2-english}
NAME=${2:-distilbert_sst2_onnx}
TASK=${3:-text-classification}

mkdir -p models
docker run --rm -v "$(pwd)/models:/models" -v huggingface-cache:/root/.cache/huggingface \
  python:3.12-slim bash -c "
  set -e
  pip install -q 'optimum[onnx]' onnx onnxruntime torch --extra-index-url https://download.pytorch.org/whl/cpu
  optimum-cli export onnx --model '$MODEL' --task '$TASK' '/models/$NAME'

  # Spark NLP expects tokenizer files and labels in assets/
  cd '/models/$NAME' && mkdir -p assets
  cp vocab.txt assets/ 2>/dev/null || true                    # BERT-like tokenizers
  cp vocab.json merges.txt assets/ 2>/dev/null || true        # RoBERTa-like (BPE) tokenizers
  python3 -c \"import json; c=json.load(open('config.json')); l=c.get('id2label',{}); \
print('\\n'.join(l[str(i)] for i in range(len(l))))\" > assets/labels.txt
  chmod -R a+rwX /models/$NAME
  ls -la /models/$NAME /models/$NAME/assets
"
