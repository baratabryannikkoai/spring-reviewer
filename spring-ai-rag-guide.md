# Building a RAG System with Spring AI: Ingestion & Retrieval

This guide walks through building a production-oriented Retrieval-Augmented Generation (RAG) system in a Spring Boot application using **Spring AI** (Java's native equivalent to the LangChain ecosystem). Where relevant, a note is included on how the same step maps to **LangChain4j**, in case you need to compare or swap frameworks.

LLM used in examples: **OpenAI** (`gpt-4o-mini` for chat, `text-embedding-3-small` for embeddings). Swapping to Gemini only requires changing the starter dependency and properties — this is called out in Section 2.

---

## 1. Architecture Overview

A RAG system has two independent pipelines that share a **Vector Store**:

```
INGESTION (offline / batch)
Source Docs → Reader → Splitter (chunking) → Enrich Metadata → Embed → Vector Store

RETRIEVAL (online / per-request)
User Query → (Query Transform) → Embed Query → Similarity Search → Vector Store
           → Rerank (optional) → Build Augmented Prompt → ChatClient → LLM → Answer
```

Spring AI models this as:
- **ETL pipeline** (`DocumentReader` → `DocumentTransformer` → `DocumentWriter`) for ingestion.
- **Advisors** (`QuestionAnswerAdvisor`, `RetrievalAugmentationAdvisor`) wrapping `ChatClient` for retrieval.

---

## 2. Project Setup

### 2.1 Dependencies (Maven)

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.springframework.ai</groupId>
      <artifactId>spring-ai-bom</artifactId>
      <version>1.1.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <!-- Chat + embedding model: OpenAI -->
  <dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-model-openai</artifactId>
  </dependency>

  <!-- Swap-in for Gemini instead of OpenAI:
  <dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-model-vertex-ai-gemini</artifactId>
  </dependency>
  -->

  <!-- Vector store: PGVector (Postgres). Alternatives: Redis, Qdrant, Milvus, Chroma -->
  <dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-vector-store-pgvector</artifactId>
  </dependency>

  <!-- Modular RAG building blocks (RetrievalAugmentationAdvisor, query transformers, rerankers) -->
  <dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-rag</artifactId>
  </dependency>

  <!-- Document readers: PDF, DOCX, HTML, PPT, etc. via Apache Tika -->
  <dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-tika-document-reader</artifactId>
  </dependency>

  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
  </dependency>
</dependencies>
```

> **LangChain equivalent:** if you were doing this in Python LangChain, this maps to `langchain-openai` + `langchain-postgres` (PGVector) + `RecursiveCharacterTextSplitter` + `PyPDFLoader`. Spring AI's `DocumentReader`/`DocumentTransformer`/`VectorStore` interfaces intentionally mirror LangChain's `Loader`/`TextSplitter`/`VectorStore` abstractions, so the concepts below transfer directly if your org standardizes on LangChain4j (Java) instead — see the callout boxes throughout.

### 2.2 `application.yml`

```yaml
spring:
  ai:
    openai:
      api-key: ${OPENAI_API_KEY}
      chat:
        options:
          model: gpt-4o-mini
          temperature: 0.2
          max-tokens: 800
      embedding:
        options:
          model: text-embedding-3-small
    vectorstore:
      pgvector:
        index-type: HNSW
        distance-type: COSINE_DISTANCE
        dimensions: 1536          # must match the embedding model's output size
        initialize-schema: true
        schema-validation: true

  datasource:
    url: jdbc:postgresql://localhost:5432/ragdb
    username: rag
    password: ${DB_PASSWORD}
```

Enable the `pgvector` extension once in Postgres: `CREATE EXTENSION IF NOT EXISTS vector;`

---

## 3. Ingestion Pipeline

### 3.1 Document representation and chunk IDs

Every unit of text in Spring AI is a `Document`:

```java
public class Document {
    private String id;                     // unique chunk identifier
    private String text;
    private Map<String, Object> metadata;   // source, page, section, etc.
    private float[] embedding;              // populated by the VectorStore
}
```

**Best practice — deterministic chunk IDs.** By default, `Document` generates a random UUID per chunk. For production ingestion, generate a **deterministic ID** derived from the source + chunk position (e.g., a hash) instead of a random UUID. This gives you:
- **Idempotent re-ingestion** — re-running the pipeline on an unchanged document upserts the same rows instead of duplicating them.
- **Safe deletes/updates** — when a source document changes, you can delete-by-id-prefix and re-insert only that document's chunks instead of wiping the whole store.
- **Traceability** — you can map a retrieved chunk straight back to "document X, chunk 7" for citations or debugging.

```java
public final class ChunkIdGenerator {

    public static String deterministicId(String sourceId, int chunkIndex, String chunkText) {
        String basis = sourceId + "::" + chunkIndex + "::" + chunkText;
        byte[] hash = DigestUtils.sha256(basis.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(hash).substring(0, 32); // stable, collision-resistant
    }
}
```

Apply this after splitting, before writing to the vector store:

```java
List<Document> idStamped = chunks.stream()
    .map(doc -> {
        int index = chunks.indexOf(doc); // for illustration; track index during split instead
        String id = ChunkIdGenerator.deterministicId(sourceId, index, doc.getText());
        return doc.mutate().id(id).build();
    })
    .toList();
```

> **LangChain equivalent:** this is the same problem LangChain's `index()` API (with a `RecordManager`) solves — deterministic hashing to support idempotent upserts against a vector store.

### 3.2 Reading source documents

Spring AI ships several `DocumentReader` implementations. Use `TikaDocumentReader` for a catch-all (PDF, DOCX, PPTX, HTML, RTF):

```java
Resource resource = new ClassPathResource("docs/employee-handbook.pdf");
DocumentReader reader = new TikaDocumentReader(resource);
List<Document> rawDocs = reader.get(); // one Document per source file (or per page, for PagePdfDocumentReader)
```

For PDFs where you need **page-level metadata** (page number is very useful for citations), use `PagePdfDocumentReader` instead:

```java
PdfDocumentReaderConfig config = PdfDocumentReaderConfig.builder()
    .withPageTopMargin(0)
    .withPageExtractedTextFormatter(ExtractedTextFormatter.defaults())
    .withPagesPerDocument(1)
    .build();

DocumentReader reader = new PagePdfDocumentReader(resource, config);
```

### 3.3 Chunking (splitting)

**Why chunk at all:** LLMs have finite context windows, embedding models have a max input length, and retrieval precision improves when each vector represents one coherent idea rather than an entire document.

Spring AI's default splitter is `TokenTextSplitter`, which splits by **token count** (not raw character count) using the model's tokenizer, and tries to respect sentence/paragraph boundaries where possible:

```java
TokenTextSplitter splitter = TokenTextSplitter.builder()
    .withChunkSize(800)              // target tokens per chunk
    .withMinChunkSizeChars(350)      // avoid tiny trailing chunks
    .withMinChunkLengthToEmbed(5)    // drop near-empty fragments
    .withMaxNumChunks(10000)         // safety cap for huge documents
    .withKeepSeparator(true)
    .build();

List<Document> chunks = splitter.apply(rawDocs);
```

**Chunking best practices:**

| Concern | Recommendation |
|---|---|
| **Chunk size** | 200–500 tokens for dense Q&A retrieval; 800–1000 tokens if you need more surrounding context per chunk (fewer, larger citations). Always chunk by **tokens**, not characters — character counts don't correlate reliably with what the embedding/chat model actually "sees," especially with non-English text or code. |
| **Overlap** | 10–20% overlap between adjacent chunks (Spring AI's splitter handles this internally) prevents a fact from being split exactly across a chunk boundary and lost from both halves. |
| **Tokenizer alignment** | Use the **same tokenizer family** the embedding model expects. `TokenTextSplitter` defaults to a `cl100k_base`-compatible encoding (OpenAI-style BPE). If you switch to Gemini, verify the splitter's token counts are still a reasonable proxy — Gemini's tokenizer differs, so treat `chunkSize` as an approximate budget, not an exact one, and leave headroom. |
| **Structure-aware splitting** | For Markdown/HTML sources, split on headers first (keep a section together) and only fall back to token splitting within an oversized section. This preserves semantic coherence better than blind token windows. |
| **Don't chunk tables/code carelessly** | Token-splitting a table or code block mid-row breaks its meaning. Extract and chunk these separately (e.g., one table = one chunk) when your source has them. |
| **Minimum chunk length** | Discard or merge trivially small chunks (headers alone, page footers) — they add noise and consume retrieval slots without value. |

### 3.4 Enriching metadata

Attach metadata **before** writing to the vector store — this is what enables filtered retrieval later (Section 4.4) and citation display:

```java
List<Document> enriched = chunks.stream()
    .map(doc -> {
        doc.getMetadata().put("source", "employee-handbook.pdf");
        doc.getMetadata().put("department", "HR");
        doc.getMetadata().put("version", "2026-08");
        doc.getMetadata().put("ingested_at", Instant.now().toString());
        return doc;
    })
    .toList();
```

### 3.5 Writing to the vector store

The `VectorStore` bean auto-wires the configured embedding model — calling `add()`/`accept()` generates embeddings and persists them in one step:

```java
@Configuration
public class RagIngestionConfig {

    private static final Logger log = LoggerFactory.getLogger(RagIngestionConfig.class);

    @Bean
    ApplicationRunner ingest(VectorStore vectorStore, ResourcePatternResolver resolver) throws IOException {
        return args -> {
            Resource[] files = resolver.getResources("classpath:docs/*.pdf");
            for (Resource file : files) {
                log.info("Ingesting {}", file.getFilename());

                var reader = new TikaDocumentReader(file);
                var splitter = TokenTextSplitter.builder().withChunkSize(500).build();

                List<Document> chunks = splitter.apply(reader.get());
                List<Document> withIds = stampDeterministicIds(chunks, file.getFilename());
                List<Document> withMetadata = enrichMetadata(withIds, file.getFilename());

                vectorStore.add(withMetadata); // embeds + upserts by id
            }
            log.info("Ingestion complete.");
        };
    }
}
```

> Run ingestion as a **separate batch job or admin endpoint** in real systems, not as an `ApplicationRunner` on every boot — you don't want to re-embed your entire corpus (and pay for it) every time the app restarts. Use a scheduled job, a file-watch trigger, or an explicit `/admin/ingest` endpoint guarded by auth, and rely on the deterministic IDs from Section 3.1 to make repeated runs cheap (unchanged chunks are simple no-op upserts).

---

## 4. Retrieval Pipeline

### 4.1 Simple RAG with `QuestionAnswerAdvisor`

The fastest path — wraps a single similarity search around the `ChatClient` call:

```java
@Service
public class RagChatService {

    private final ChatClient chatClient;

    public RagChatService(ChatClient.Builder builder, VectorStore vectorStore) {
        this.chatClient = builder
            .defaultAdvisors(QuestionAnswerAdvisor.builder(vectorStore)
                .searchRequest(SearchRequest.builder()
                    .similarityThreshold(0.75)
                    .topK(5)
                    .build())
                .build())
            .build();
    }

    public String ask(String question) {
        return chatClient.prompt()
            .user(question)
            .call()
            .content();
    }
}
```

`QuestionAnswerAdvisor` embeds the user's question, runs a similarity search, and injects the retrieved chunks into the system prompt automatically using a default RAG template ("Context information is below... Given the context, answer the query...").

### 4.2 Modular RAG with `RetrievalAugmentationAdvisor`

For production systems, prefer `RetrievalAugmentationAdvisor` (from `spring-ai-rag`) — it lets you compose query transformation, retrieval, and post-processing as separate, swappable stages:

```java
@Bean
public Advisor ragAdvisor(VectorStore vectorStore, ChatClient.Builder chatClientBuilder) {

    var documentRetriever = VectorStoreDocumentRetriever.builder()
        .vectorStore(vectorStore)
        .similarityThreshold(0.60)
        .topK(6)
        .build();

    var queryCompressor = CompressionQueryTransformer.builder()
        .chatClientBuilder(chatClientBuilder)
        .build(); // rewrites a follow-up question into a standalone query using chat history

    return RetrievalAugmentationAdvisor.builder()
        .queryTransformers(queryCompressor)
        .documentRetriever(documentRetriever)
        .build();
}
```

```java
@Service
public class ModularRagService {

    private final ChatClient chatClient;

    public ModularRagService(ChatClient.Builder builder, Advisor ragAdvisor) {
        this.chatClient = builder.defaultAdvisors(ragAdvisor).build();
    }

    public String ask(String question, List<Message> history) {
        Query query = Query.builder().text(question).history(history).build();
        return chatClient.prompt()
            .advisors(ragAdvisor)
            .user(question)
            .call()
            .content();
    }
}
```

> **LangChain equivalent:** `RetrievalAugmentationAdvisor` plays the role of a LangChain `RetrievalQA` / LCEL retrieval chain — a pipeline of `QueryTransformer → DocumentRetriever → DocumentPostProcessor → PromptTemplate`. If your team already has LangChain (Python) retrieval chains and is porting to Java, LangChain4j's `RetrievalAugmentor` is the closer 1:1 analog; Spring AI's version integrates more tightly with `ChatClient`/Advisor chaining if the rest of your app is already Spring AI-based.

### 4.3 Adding a reranking step

Vector similarity alone often over-retrieves "related but not quite relevant" chunks. Add a `DocumentPostProcessor` to rerank the top-K candidates with a cross-encoder before they reach the prompt:

```java
var rerankingAdvisor = RetrievalAugmentationAdvisor.builder()
    .documentRetriever(VectorStoreDocumentRetriever.builder()
        .vectorStore(vectorStore)
        .topK(20)                 // over-fetch...
        .build())
    .documentPostProcessors(new CohereRerankPostProcessor(cohereClient, /* topN */ 5)) // ...then rerank down to 5
    .build();
```

### 4.4 Metadata filtering

Restrict retrieval to a subset of the corpus using Spring AI's portable filter expression (translated to native `WHERE`/filter syntax per vector store):

```java
Filter.Expression filter = new FilterExpressionBuilder()
    .and(
        new FilterExpressionBuilder().eq("department", "HR"),
        new FilterExpressionBuilder().gte("version", "2026-01")
    ).build();

var documentRetriever = VectorStoreDocumentRetriever.builder()
    .vectorStore(vectorStore)
    .filterExpression(filter)
    .topK(5)
    .build();
```

This is essential in multi-tenant or access-controlled RAG (e.g., only retrieve chunks the requesting user's role is permitted to see) — always filter server-side by trusted metadata, never by a client-supplied string concatenated into a query.

### 4.5 Grounding and hallucination control

- Use a **custom prompt template** on the advisor that explicitly instructs the model to say "I don't know" when the context doesn't contain the answer, and to avoid meta-commentary like "based on the provided context":

```java
PromptTemplate qaPromptTemplate = PromptTemplate.builder()
    .template("""
        You are a helpful assistant answering questions using ONLY the context below.
        If the answer is not contained in the context, say you don't know — do not guess.

        Context:
        {question_answer_context}

        Question:
        {query}
        """)
    .build();

QuestionAnswerAdvisor qaAdvisor = QuestionAnswerAdvisor.builder(vectorStore)
    .promptTemplate(qaPromptTemplate)
    .build();
```

- Return **citations** by surfacing each retrieved `Document`'s metadata (source, page, chunk id from Section 3.1) alongside the answer, rather than trusting the model to cite accurately on its own.
- Log the retrieved chunk IDs per request (Section 6) so a bad answer can be traced back to whether it was a **retrieval failure** (wrong/no chunks found) or a **generation failure** (right chunks, model still got it wrong).

---

## 5. Structured Output from Retrieval-Augmented Calls

Combine RAG with Spring AI's structured output converter so the final answer is a typed object, not raw text:

```java
public record Answer(String summary, List<String> citedSources, double confidence) {}

Answer result = chatClient.prompt()
    .advisors(ragAdvisor)
    .user(question)
    .call()
    .entity(Answer.class);
```

Spring AI appends format instructions to the prompt and parses/validates the JSON response into `Answer` automatically — pair this with the input/output validation practices from the review checklist (Bean Validation on the record, defensive handling of malformed JSON).

---

## 6. Observability & Evaluation

- **Trace every retrieval**: log `topK`, `similarityThreshold`, the retrieved chunk IDs, and their similarity scores per request — this is your primary debugging tool when answers are wrong.
- **Evaluate retrieval quality separately from generation quality.** Spring AI provides `RelevancyEvaluator` and `FactCheckingEvaluator` for automated checks:

```java
EvaluationRequest evalRequest = new EvaluationRequest(question, retrievedDocs, answer);
EvaluationResponse relevancy = new RelevancyEvaluator(chatClientBuilder).evaluate(evalRequest);
assert relevancy.isPass();
```

- Track **embedding drift**: if you change the embedding model, you must re-embed the entire corpus — old and new embeddings are not comparable in the same vector space.
- Track **cost/latency** per stage (embed query, similarity search, rerank, generate) as separate metrics/spans; the generation call is usually the largest cost and latency contributor, so know when reranking or query transformation adds enough retrieval-quality value to justify their added latency.

---

## 7. Summary Checklist

- [ ] Chunking by **tokens**, not characters, sized to the retrieval use case (200–500 for precision, 800–1000 for context)
- [ ] **Deterministic chunk IDs** for idempotent re-ingestion and traceable citations
- [ ] Metadata attached at ingestion time (source, section, version, access-control tags)
- [ ] Ingestion run as a **separate batch process**, not on every app boot
- [ ] Retrieval uses `similarityThreshold` + `topK` tuned per corpus, not left at library defaults
- [ ] Server-side metadata filtering enforced for multi-tenant/access-controlled data
- [ ] Custom prompt template that permits "I don't know" instead of forcing an answer
- [ ] Citations surfaced from retrieved chunk metadata, not model self-reporting
- [ ] Structured output (`.entity(Class)`) used when the answer needs to be consumed programmatically
- [ ] Retrieval and generation quality evaluated and logged separately
