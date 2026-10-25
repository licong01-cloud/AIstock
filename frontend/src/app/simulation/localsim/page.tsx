"use client";
import { useEffect } from "react";
import { useRouter } from "next/navigation";
export default function LocalSimHome(){
 const router=useRouter();
 useEffect(()=>{const q=new URLSearchParams(window.location.search),packageId=q.get("package_id"),topK=Number(q.get("top_k"));if(packageId){sessionStorage.setItem("localsim-configure-selection",JSON.stringify({packageId,topK}));router.replace("/simulation/localsim/configure");}else router.replace("/simulation/localsim/workbench");},[router]);
 return <p>正在打开模拟盘…</p>;
}