import ReactMarkdown from "react-markdown";

const AiMarkdownSummary = ({
  loading,
  error,
  markdown,
}: {
  loading: boolean;
  error: string | null;
  markdown: string | null | undefined;
}) => {
  if (loading) {
    return <div className="p-5 text-sm text-charcoal-400">Generating insights…</div>;
  }

  if (error) {
    return (
      <div className="p-5 text-sm text-red-600 bg-red-50 border border-red-200 rounded-xl">
        {error}
      </div>
    );
  }

  if (!markdown) return null;

  return (
    <div className="prose prose-sm max-w-none">
      <ReactMarkdown>{markdown}</ReactMarkdown>
    </div>
  );
};

export default AiMarkdownSummary;
