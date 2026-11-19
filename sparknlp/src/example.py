# Import Spark NLP
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp as sparknlp
import os

# Suppress Ivy logs
os.environ['IVY_LOG_LEVEL'] = 'error'

# Start SparkSession with Spark NLP
# start() functions has 3 parameters: gpu, apple_silicon, and memory
# sparknlp.start(gpu=True) will start the session with GPU support
# sparknlp.start(apple_silicon=True) will start the session with macOS M1 & M2 support
# sparknlp.start(memory="16G") to change the default driver memory in SparkSession
print("Starting Spark session...")
spark = sparknlp.start(memory="8G")   # add gpu=True on a CUDA machine

# Set Spark log level to ERROR to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")
print("Spark session started successfully!", "Spark NLP", sparknlp.version(), "- Spark", spark.version)

# Download a pre-trained pipeline (using a lighter pipeline)
print("\nDownloading pre-trained pipeline...")
pipeline = PretrainedPipeline('recognize_entities_dl', lang='en')
print("Pipeline loaded successfully!")

# Your testing dataset
text = """
The Mona Lisa is a 16th century oil painting created by Leonardo.
It's held at the Louvre in Paris.
"""
text = """ 
Colombia's President Gustavo Petro has told the BBC that he believes there is now a "real threat" of US military action against Colombia.

Petro said the United States is treating other nations as part of a US "empire". It comes after Trump threatened Colombia with military action. He said that the US risks transforming from "dominating the world" to becoming "isolated from the world."

He also accused US Immigration and Customs Enforcement (ICE) agents of acting like "Nazi brigades". Trump has significantly expanded ICE operations as part of what the administration says is a crackdown on crime and immigrants who illegally entered the US.

The BBC has approached the White House for comment.

Following US strikes on Venezuela and the seizure of Nicolás Maduro, US President Donald Trump said a military operation targeting Colombia "sounds good".

Trump has also repeatedly told Petro to "watch his ass", remarks Petro strongly condemned.

Trump and Petro spoke by phone on Wednesday evening, after which Trump said he would meet his Colombian counterpart at the White House in the "near future. Writing on his Truth Social platform late on Wednesday after the call, Trump described his conversation with Petro as a "Great Honour". A Colombian official said at the time that the conversation had reflected a 180-degree shift in rhetoric "from both sides."

But on Thursday, Petro's tone suggested relations had not significantly improved.

He told the BBC the call lasted just under an hour, "most of it occupied by me," and covered "drug trafficking Colombia" and Colombia's view on Venezuela and "what is happening around Latin America regarding the United States."

Petro strongly criticised recent US immigration enforcement, accusing ICE agents of operating like "Nazi brigades".

President Trump has often blamed immigration for crime and trafficking in the US, using it to justify large-scale enforcement operations, and has accused countries like Colombia and Venezuela of not doing enough to tackle drug-trafficking.

Since returning to the White House, the US president has sent ICE agents to cities across the country. The agency enforces immigration laws and conducts investigations into undocumented immigration. It also plays a role in removing undocumented immigrants from the US.

The administration says it deported 605,000 people between 20 January and 10 December 2025. It also said 1.9 million immigrants had "voluntarily self-deported", following an aggressive public awareness campaign encouraging people to leave the country on their own to avoid arrest or detention.

About 65,000 people were in ICE detention as of 30 November 2025, according to data obtained by the Transactional Records Access Clearinghouse's immigration project, a compendium of government data from Syracuse University.

This week a US immigration agent shot dead a 37-year-old US citizen in the city of Minneapolis, sparking protests overnight.

Federal officials said the woman, Renee Nicole Good, had tried to run over immigration agents with her car but the city mayor, Democrat Jacob Frey, said the agent who shot her had acted recklessly and demanded agents leave the city.

Petro said ICE had "reached the point where it no longer only persecutes Latin Americans in the streets, which for us is an affront, but it also kills United States citizens."

He added that if this continued, "instead of a United States dominating the world – an imperial dream – it is a United States isolated from the world. An empire was not built by being isolated from the world."

Petro said the US has for "decades" treated other governments, particularly in Latin America, as an "empire" regardless of the law.
"""
# Annotate your testing dataset
print("\nAnnotating text...")
# fullAnnotate() keeps offsets and metadata (e.g. the entity label),
# annotate() would return only plain strings
result = pipeline.fullAnnotate(text)[0]
print("Annotation completed!")
print("Output columns:", ", ".join(result.keys()))

# ANSI colors, one per entity type (PER, LOC, ORG, MISC)
COLORS = {"PER": "\033[1;34m", "LOC": "\033[1;32m", "ORG": "\033[1;35m", "MISC": "\033[1;33m"}
RESET = "\033[0m"

def color(label, s):
    return f"{COLORS.get(label, '')}{s}{RESET}"

def section(title):
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")

sentences = result["sentence"]
tokens = result["token"]
entities = result["entities"]

# 1. Summary
section("SUMMARY")
print(f"Characters: {len(text)}  Sentences: {len(sentences)}  "
      f"Tokens: {len(tokens)}  Entities: {len(entities)}")

# 2. Entities grouped by type, with number of mentions
section("ENTITIES BY TYPE")
from collections import Counter, defaultdict
by_type = defaultdict(Counter)
for e in entities:
    by_type[e.metadata["entity"]][e.result] += 1
for label, counter in sorted(by_type.items()):
    print(color(label, f"{label} ({sum(counter.values())})"))
    for name, n in counter.most_common():
        print(f"   {n:>3} x {name}")

# 3. Original text with entities highlighted in place
section("TEXT WITH HIGHLIGHTED ENTITIES")
out, pos = [], 0
for e in sorted(entities, key=lambda e: e.begin):
    label = e.metadata["entity"]
    out.append(text[pos:e.begin])
    out.append(color(label, f"[{text[e.begin:e.end + 1]}]{label}"))
    pos = e.end + 1
out.append(text[pos:])
print("".join(out).strip())

# 4. Token-level view of one sentence: how NER tags each token (IOB format)
section("TOKEN / NER TAGS (first sentence)")
first = sentences[0]
print(first.result, "\n")
print(f"{'TOKEN':<20}{'TAG'}")
for tok, tag in zip(tokens, result["ner"]):
    if tok.begin > first.end:
        break
    label = tag.result.split("-")[-1]
    print(f"{tok.result:<20}" + (color(label, tag.result) if tag.result != "O" else "O"))

# Stop Spark session
print("\nStopping Spark session...")
spark.stop()
print("Done!")
