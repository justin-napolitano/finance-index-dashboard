import Link from 'next/link';

export default function Home() {
  return (
    <main style={{padding: 24}}>
      <h1>Finance Index Dashboard</h1>
      <p>Explore dynamic indices and live performance.</p>
      <p><Link href="/indices">Go to Indices →</Link></p>
      <div id="ad-slot-top" style={{width:'100%',height:90,background:'#f3f3f3',marginTop:24,display:'flex',alignItems:'center',justifyContent:'center'}}>
        {/* Google AdSense placeholder */}
        <span>Ad slot</span>
      </div>
    </main>
  );
}
