/*export default function ConnectPage() {
  const region = process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu";
  return (
    <main className="p-8">
      <h1 className="text-2xl font-semibold mb-4">Amazon verbinden</h1>
      <a
        className="inline-block rounded px-4 py-2 border"
        href={`/api/amazon/connect?region=${region}`}
      >
        Mit Amazon verbinden
      </a>
    </main>
  );
}*/


import { redirect } from "next/navigation";
export default function Page() { redirect("/dashboard"); }
