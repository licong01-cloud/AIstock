"use client";
import { useParams } from "next/navigation";
import { DualModeComparison } from "../../components/dual-mode-comparison";

export default function ComparisonPage(){
 const studyId=String(useParams<{studyId:string}>().studyId||"");
 return <DualModeComparison studyId={studyId}/>;
}
