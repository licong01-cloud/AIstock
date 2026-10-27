import Link from "next/link";
import AdvisoryManagement from "@/components/advisory/AdvisoryManagement";

export default function AdvisoryManagementPage() {
  return <><Link href="/paper-v2/advisory">← 返回荐股总览</Link><AdvisoryManagement /></>;
}
