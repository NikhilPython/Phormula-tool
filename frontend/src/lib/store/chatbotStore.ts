import { create } from "zustand";
import { v4 as uuid } from "uuid";

export type Message = {
  id: string                 // ✅ CLIENT ID (uuid)
  sender: "user" | "bot"
  text: string

  // backend related
  serverId?: number          // ✅ SERVER ID
  promptText?: string

  // ui
  liked?: "like" | "dislike"
  error?: boolean
  timestamp?: number
}
type ChatStore = {
  messages: Message[];
  loading: boolean;
  loadFromStorage: () => void;
  sendMessage: (text: string) => Promise<void>;
  clearChat: () => void;
  reactToMessage: (id: string, reaction: Message["liked"]) => void;
  sendFeedback: (
    id: string,
    feedback: "like" | "dislike",
    additional_feedback?: string
  ) => Promise<void>;
};

const DEFAULT_BOT_MESSAGE = {
  id: "welcome-message",
  sender: "bot",
  text: "Hey! 👋 I can help you analyze Amazon sales, fees, taxes, profit, and trends. What would you like to explore?",
  timestamp: Date.now(),
};

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:5000";

const getAuthToken = () =>
  typeof window !== "undefined"
    ? localStorage.getItem("jwtToken")
    : null;

export const useChatbotStore = create<ChatStore>((set, get) => ({
  messages: [],
  loading: false,

  

  loadFromStorage: () => {
  const saved = localStorage.getItem("chatbot_history");

  // agar pehle se messages loaded hain → kuch mat karo
  set((state) => {
    if (state.messages.length > 0) return state;

    // Case 1: localStorage me data hai
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length > 0) {
          return { messages: parsed };
        }
      } catch {
        // ignore parse error
      }
    }

    // Case 2: first time / empty / no storage
    return { messages: [DEFAULT_BOT_MESSAGE] };
  });
},


  sendMessage: async (text) => {
    if (!text.trim()) return;

    const userMsg: Message = {
  id: uuid(),
  sender: "user",
  text,
  timestamp: Date.now(),
};

    set((s) => ({
      messages: [...s.messages, userMsg],
      loading: true,
    }));

    try {
      const res = await fetch(`${API_BASE_URL}/chatbot`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getAuthToken()}`,
        },
        body: JSON.stringify({
          action: "chat",
          query: text,
        }),
      });

      const data = await res.json();

      const botMsg: Message = {
  id: uuid(),
  sender: "bot",
  text: data?.response || "No response",
  timestamp: Date.now(),
  
};

      set((s) => ({
        messages: [...s.messages, botMsg],
        loading: false,
      }));

      localStorage.setItem(
        "chatbot_history",
        JSON.stringify([...get().messages, botMsg])
      );
    } catch {
      set({ loading: false });
    }
  },

  clearChat: () => {
    localStorage.removeItem("chatbot_history");
    set({ messages: [] });
  },

  reactToMessage: (id, reaction) => {
    set((s) => {
      const updated = s.messages.map((m) =>
        m.id === id ? { ...m, liked: reaction } : m
      );
      // keep storage in sync for both page + window
      if (typeof window !== "undefined") {
        localStorage.setItem("chatbot_history", JSON.stringify(updated));
      }
      return { messages: updated };
    });
  },

  sendFeedback: async (id, feedback, additional_feedback) => {
    const msg = get().messages.find((m) => m.id === id);
    if (!msg?.serverId) return;

    try {
      await fetch(`${API_BASE_URL}/chatbot/feedback`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${getAuthToken()}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          message_id: msg.serverId,
          feedback,
          original_prompt: msg.promptText,
          additional_feedback,
        }),
      });
    } catch {
      // ignore (UI should still feel responsive)
    }
  },
}));