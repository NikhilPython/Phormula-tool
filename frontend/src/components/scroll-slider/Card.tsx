'use client';

import Image from 'next/image';
import styles from './style.module.scss';
import { useTransform, motion } from 'framer-motion';

interface CardProps {
  i: number;
  title: string;
  items: Array<{
    title: string;
    description: string;
    image: string;
  }>;
  highlighterImage: string;
  progress: any;
  range: [number, number];
  targetScale: number;
}

const Card: React.FC<CardProps> = ({
  i,
  title,
  items,
  highlighterImage,
  progress,
  range,
  targetScale,
}) => {
  const scale = useTransform(progress, range, [1, targetScale]);

  return (
    <div className={styles.cardContainer}>
      <motion.div
        style={{
          scale,
         top: `calc(86px + ${i * 18}px)`
        }}
        className={styles.card}
      >
        <Image
          src={highlighterImage}
          alt="highlighter"
          width={220}
          height={220}
          className={`absolute bottom-0 pointer-events-none ${
            i % 2 === 0 ? 'right-0' : 'left-0'
          }`}
        />

        <h2 className="text-start text-2xl font-bold text-primary-300 relative z-10">
          {title}
        </h2>

        <div className={styles.body}>
          <div className={styles.itemsWrap}>
            {items.map((item, index) => (
              <div key={index} className={styles.itemCard}>
                <Image
                  src={item.image}
                  alt={item.title}
                  width={220}
                  height={220}
                />

                <h3>{item.title}</h3>
                <p>{item.description}</p>
              </div>
            ))}
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default Card;