import { useState, useEffect } from 'react';

export function useScrollFade(fadeStart: number = 50, fadeEnd: number = 150) {
  const [opacity, setOpacity] = useState(1);

  useEffect(() => {
    const handleScroll = () => {
      const scrollY = window.scrollY;
      
      if (scrollY <= fadeStart) {
        setOpacity(1);
      } else if (scrollY >= fadeEnd) {
        setOpacity(0);
      } else {
        // Calculate opacity between fadeStart and fadeEnd
        const fadeRange = fadeEnd - fadeStart;
        const scrollProgress = scrollY - fadeStart;
        const newOpacity = 1 - (scrollProgress / fadeRange);
        setOpacity(Math.max(0, Math.min(1, newOpacity)));
      }
    };

    window.addEventListener('scroll', handleScroll, { passive: true });
    return () => window.removeEventListener('scroll', handleScroll);
  }, [fadeStart, fadeEnd]);

  return opacity;
}