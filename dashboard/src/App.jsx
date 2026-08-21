import { useState, useEffect } from 'react'
import reactLogo from './assets/react.svg'
import viteLogo from './assets/vite.svg'
import heroImg from './assets/hero.png'
import './App.css'

function App() {

  const [greeks, setGreeks] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      const response = await fetch('http://localhost:8000/greeks/SPY')
      const json = await response.json();
      setGreeks(json);
    };
    fetchData();
  }, []);

  return (
    <div>
      {greeks ? JSON.stringify(greeks) : 'Loading...'}
    </div>
  );

}

export default App
