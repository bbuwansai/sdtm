"use client";

import { motion, useMotionValue, useSpring } from "framer-motion";
import { useEffect, useState } from "react";

export function CustomCursor() {
  const [visible, setVisible] = useState(false);
  const x = useMotionValue(-100);
  const y = useMotionValue(-100);

  const springX = useSpring(x, { stiffness: 280, damping: 28, mass: 0.4 });
  const springY = useSpring(y, { stiffness: 280, damping: 28, mass: 0.4 });

  useEffect(() => {
    const move = (event: MouseEvent) => {
      x.set(event.clientX - 12);
      y.set(event.clientY - 12);
      setVisible(true);
    };

    const hide = () => setVisible(false);

    window.addEventListener("mousemove", move);
    window.addEventListener("mouseleave", hide);

    return () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseleave", hide);
    };
  }, [x, y]);

  return (
    <motion.div
      aria-hidden="true"
      className="pointer-events-none fixed left-0 top-0 z-[100] hidden h-6 w-6 rounded-full border border-emerald-300/60 bg-emerald-300/10 mix-blend-screen md:block"
      style={{ x: springX, y: springY, opacity: visible ? 1 : 0 }}
    >
      <div className="absolute inset-0 rounded-full shadow-[0_0_40px_rgba(52,211,153,0.35)]" />
    </motion.div>
  );
}
