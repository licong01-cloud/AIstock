"use client";
import { useParams } from "next/navigation";
import { LegacyAccountEntry } from "../../../components/trading-workbench";
export default function LegacyPage(){const id=String(useParams<{accountId:string}>().accountId||"");return <LegacyAccountEntry accountId={id} tab="pnl"/>;}
