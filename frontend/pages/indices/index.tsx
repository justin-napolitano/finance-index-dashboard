import { useEffect, useState } from 'react';
import axios from 'axios';
import Link from 'next/link';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export default function Indices() {
  const [data, setData] = useState<any[]>([]);
  useEffect(() => { axios.get(`${API}/indices/`).then(r => setData(r.data)).catch(() => setData([])); }, []);
  return (
    <main style={{padding:24}}>
      <h1>Indices</h1>
      <ul>
        {data.map((d) => (
          <li key={d.id}><Link href={`/indices/${d.slug}`}>{d.name}</Link></li>
        ))}
      </ul>
      <div id="ad-slot-list" style={{width:'100%',height:250,background:'#f3f3f3',marginTop:24,display:'flex',alignItems:'center',justifyContent:'center'}}>
        <span>Ad slot</span>
      </div>
    </main>
  );
}
