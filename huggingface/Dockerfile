FROM apache/spark:4.3.0-python3

USER root

# Hugging Face libraries; CPU-only PyTorch keeps the image ~2 GB instead of ~8 GB
RUN pip install --no-cache-dir \
    "pandas<3" \
    pyarrow \
    transformers \
    "optimum[onnxruntime]" \
    torch --extra-index-url https://download.pytorch.org/whl/cpu

# Cache directory for Hugging Face models (mount a volume here)
RUN mkdir -p /opt/spark/.cache/huggingface && \
    chown -R spark:spark /opt/spark/.cache
ENV HF_HOME=/opt/spark/.cache/huggingface

USER spark
