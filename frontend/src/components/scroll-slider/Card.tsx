"use client";

import Image from "next/image";
import styles from "./style.module.scss";
import {
  motion,
  useTransform,
  type MotionStyle,
  type MotionValue,
} from "framer-motion";

interface CardProps {
  i: number;
  title: string;
  items: Array<{
    title: string;
    description: string;
    image: string;
  }>;
  highlighterImage?: string;
  progress: MotionValue<number>;
  range: [number, number];
  targetScale: number;
}

type CardMotionStyle = MotionStyle & {
  "--card-index": number;
};

const Card: React.FC<CardProps> = ({
  i,
  title,
  items,
  progress,
  range,
  targetScale,
}) => {
  const scale = useTransform(progress, range, [1, targetScale]);

  const renderHighlightedTitle = (heading: string) => {
    switch (heading) {
      case "Real-Time Financial Analysis":
        return (
          <>
            Real-Time{" "}
            <span className={styles.highlightedTitle}>
              Financial Analysis
            </span>
          </>
        );

      case "Inventory Forecasting":
        return (
          <>
            Inventory{" "}
            <span className={styles.highlightedTitle}>
              Forecasting
            </span>
          </>
        );

      case "Expense, Fee & Cash-Flow Visibility":
        return (
          <>
            Expense, Fee & Cash-Flow{" "}
            <span className={styles.highlightedTitle}>
              Visibility
            </span>
          </>
        );

      case "AI Business Insights":
        return (
          <>
            <span className={styles.highlightedTitle}>AI</span>{" "}
            Business Insights
          </>
        );

      default:
        return heading;
    }
  };

  return (
    <div className={styles.cardContainer}>
      <motion.article
        style={
          {
            scale,
            "--card-index": i,
          } as CardMotionStyle
        }
        className={styles.card}
      >
        <h2 className={styles.cardTitle}>
          {renderHighlightedTitle(title)}
        </h2>

        <div className={styles.body}>
          <div className={styles.itemsWrap}>
            {items.map((item) => (
              <div key={item.title} className={styles.itemCard}>
                <div className={styles.iconWrap}>
                  <Image
                    src={item.image}
                    alt={item.title}
                    width={140}
                    height={140}
                    className={styles.icon}
                  />
                </div>

                <h3>{item.title}</h3>

                <p className="text-left!">{item.description}</p>
              </div>
            ))}
          </div>
        </div>
      </motion.article>
    </div>
  );
};

export default Card;