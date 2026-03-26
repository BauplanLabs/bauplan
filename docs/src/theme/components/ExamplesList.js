import ExampleCard from "./ExampleCard";

const examplesData = [
  {
    id: 'data_product',
    title: "Serverless Data Product",
    description: "Serverless data product with built-in quality checks using Lambda and Bauplan.",
    href: "/examples/data_product",
    imageUrl: "/img/examples/13.png",
    tags: ["dataprod", "lambda"]
  },
  {
    id: 'rag',
    title: 'RAG system with Pinecone',
    description: 'Build a RAG system with Pinecone and OpenAI over StackOverflow data.',
    href: '/examples/rag',
    imageUrl: '/img/examples/11.png',
    tags: ["pinecone", "openAI"]
  },
  {
    id: 'medallion',
    title: 'Medallion Architecture + WAP Pattern',
    description: 'End-to-end data engineering repo using Mage & the medallion architecture.',
    href: '/examples/medallion',
    imageUrl: '/img/examples/12.png',
    tags: ["medallion", "mage", "polars"]
  },
  {
    id: 'llm_tabular',
    title: 'From unstructured to structured data with LLMs',
    description: 'Convert PDFs into structured, analyzable tables using LLMs.',
    href: '/examples/llm_tabular',
    imageUrl: '/img/examples/8.png',
    tags: ["openAI", "PDF processing", "unstructured to structured"]
  },
  {
    id: 'mongo',
    title: 'Playlist recommendations with MongoDB',
    description: 'Embedding-based recommender system for music playlists.',
    href: '/examples/mongo',
    imageUrl: '/img/examples/7.png',
    tags: ["mongoDB", "vector search", "recs"]
  },
  {
    id: 'import_data',
    title: 'Iceberg Lakehouse Pipeline',
    description: 'Orchestrated WAP pattern for ingesting parquet files to Iceberg tables.',
    href: '/examples/import_data',
    imageUrl: '/img/examples/4.png',
    tags: ["prefect", "pandas", "iceberg"]
  },
  {
    id: 'pdf_analysis_openai',
    title: 'PDF analysis with bauplan and OpenAI',
    description: 'Analyze PDFs using Bauplan for data preparation and OpenAI\'s GPT for text analysis',
    href: '/examples/pdf_analysis_openai',
    imageUrl: '/img/examples/9.png',
    tags: ["PDF processing", "openAI"]
  },
  {
    id: 'ML_pipeline',
    title: 'ML Model Training and Deployment Pipeline',
    description: 'End-to-end ML pipeline for predicting taxi trip tips.',
    href: '/examples/ML_pipeline',
    imageUrl: '/img/examples/1.png',
    tags: ["scikit-learn", "pandas", "notebooks", "streamlit"]
  },
  {
    id: 'llm',
    title: 'Entity Matching with OpenAI',
    description: 'Product matching across e-commerce catalogs using LLMs.',
    href: '/examples/llm',
    imageUrl: '/img/examples/2.png',
    tags: ["openAI", "streamlit", "pandas", "duckDB"]
  },
  {
    id: 'expectations',
    title: 'Data Quality and Expectations',
    description: 'Implement data quality checks using expectations.',
    href: '/examples/expectations',
    imageUrl: '/img/examples/3.png',
    tags: ["pyArrow", "pandas", "duckDB"]
  },
  {
    id: 'nrt',
    title: 'Near Real-time Analytics',
    description: 'Build near real-time analytics pipeline with WAP pattern and metrics visualization.',
    href: '/examples/nrt',
    imageUrl: '/img/examples/6.png',
    tags: ["prefect", "streamlit", "duckDB"]
  },
  {
    id: 'data_app',
    title: 'Interactive Data Dashboard',
    description: 'Build an interactive dashboard to visualize taxi pickup locations in NYC.',
    href: '/examples/data_app',
    imageUrl: '/img/examples/5.png',
    tags: ["streamlit", "pandas"]
  }
];

export default function ExamplesList() {
  return (
    <div className="w-full flex items-center justify-center py-8">
      <div className="max-w-8xl px-4 w-full">
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-6 auto-rows-fr">
          {examplesData.map((item) => (
            <ExampleCard
              key={item.id}
              title={item.title}
              description={item.description}
              href={item.href}
              imageUrl={item.imageUrl}
              tags={item.tags}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
