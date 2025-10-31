import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import axios from 'axios';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export default function IndexPage() {
  const router = useRouter();
  const { slug } = router.query;
  const [data, setData] = useState<any>(null);

  useEffect(() => {
    if (!slug) return;
    axios.get(`${API}/indices/${slug}`).then(r => setData(r.data)).catch(() => setData(null));
  }, [slug]);

  const chartData = (data?.history || []).map((d:any) => ({
    date: d.date,
    level: Number(d.level)
  }));

  return (
    <main style={{padding:24}}>
      <h1>{data?.meta?.name || 'Index'}</h1>
      <p>{data?.meta?.description}</p>

      <div style={{width:'100%', height:300, background:'#fff', border:'1px solid #eee', padding:8}}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <XAxis dataKey="date" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="level" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <h3 style={{marginTop:24}}>Holdings</h3>
      <ul>
        {(data?.holdings || []).map((h:any) => (
          <li key={h.ticker}>
            {h.ticker} — {(Number(h.weight)*100).toFixed(2)}%
          </li>
        ))}
      </ul>

      <div id="ad-slot-detail" style={{width:'100%',height:250,background:'#f3f3f3',marginTop:24,display:'flex',alignItems:'center',justifyContent:'center'}}>
        <span>Ad slot</span>
      </div>
    </main>
  );
}
