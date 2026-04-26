export default function JsonPanel({ value }: { value: unknown }) {
  return <pre className="pv2-json">{JSON.stringify(value, null, 2)}</pre>;
}
