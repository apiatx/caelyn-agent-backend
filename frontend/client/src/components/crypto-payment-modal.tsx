import { useState } from "react";
import { X, Copy, CheckCircle, Clock, Wallet } from "lucide-react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";

interface CryptoPaymentModalProps {
  isOpen: boolean;
  onClose: () => void;
  paymentType: 'ETH' | 'TAO' | null;
  amount: string;
  usdValue: string;
}

export function CryptoPaymentModal({ 
  isOpen, 
  onClose, 
  paymentType, 
  amount, 
  usdValue 
}: CryptoPaymentModalProps) {
  const [paymentStatus, setPaymentStatus] = useState<'pending' | 'confirming' | 'confirmed'>('pending');
  const [copied, setCopied] = useState(false);
  const { toast } = useToast();

  // Mock wallet addresses - in production these would be your actual receiving addresses
  const walletAddresses = {
    ETH: '0x742d35cc6bf9f2E7C2E4A123456789ABCDEF0123',  // BASE network address
    TAO: '5FhKPGFkiWVJWEWHQWHCtJ9EaYzfk9YZJzJyKaQxABCDEF'   // TAO network address
  };

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      toast({
        title: "Address copied!",
        description: "Payment address copied to clipboard",
      });
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      toast({
        title: "Failed to copy",
        description: "Please copy the address manually",
        variant: "destructive",
      });
    }
  };

  const handlePaymentConfirmation = () => {
    setPaymentStatus('confirming');
    // Simulate payment confirmation delay
    setTimeout(() => {
      setPaymentStatus('confirmed');
      setTimeout(() => {
        toast({
          title: "Payment Confirmed!",
          description: "Premium whale watching access activated",
        });
        onClose();
      }, 2000);
    }, 3000);
  };

  if (!paymentType) return null;

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-md glass-card-dark border-white/[0.08]">
        <DialogHeader>
          <DialogTitle className="text-xl font-bold text-white flex items-center">
            <Wallet className="mr-3 h-6 w-6 text-crypto-warning" />
            Premium Payment
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-6 mt-6">
          {/* Payment Details */}
          <GlassCard className="p-4">
            <div className="text-center">
              <div className={`w-16 h-16 rounded-full mx-auto mb-3 flex items-center justify-center text-2xl font-bold ${
                paymentType === 'ETH' 
                  ? 'bg-blue-500' 
                  : 'bg-gradient-to-r from-purple-500 to-pink-500'
              }`}>
                {paymentType}
              </div>
              <h3 className="text-lg font-semibold text-white">Send {amount} {paymentType}</h3>
              <p className="text-crypto-silver text-sm">≈ {usdValue}</p>
            </div>
          </GlassCard>

          {paymentStatus === 'pending' && (
            <>
              {/* Payment Address */}
              <div className="space-y-3">
                <label className="text-crypto-silver text-sm font-medium">
                  {paymentType === 'ETH' ? 'BASE Network Address' : 'TAO Network Address'}
                </label>
                <div className="flex items-center space-x-2">
                  <input
                    type="text"
                    value={walletAddresses[paymentType]}
                    readOnly
                    className="flex-1 bg-white/5 border border-white/[0.06] rounded-lg px-3 py-2 text-white text-sm font-mono"
                  />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => copyToClipboard(walletAddresses[paymentType])}
                    className="text-crypto-silver hover:text-white"
                  >
                    {copied ? <CheckCircle className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                  </Button>
                </div>
              </div>

              {/* Network Warning */}
              <div className="bg-crypto-warning/10 border border-crypto-warning/30 rounded-lg p-3">
                <p className="text-crypto-warning text-xs font-medium">
                  ⚠️ {paymentType === 'ETH' 
                    ? 'Ensure you send ETH on the BASE network only. Sending on other networks will result in loss of funds.'
                    : 'Ensure you send TAO on the Bittensor network only. Double-check the address before sending.'
                  }
                </p>
              </div>

              {/* Payment Instructions */}
              <div className="space-y-2">
                <h4 className="text-white font-medium">Payment Instructions:</h4>
                <ol className="text-crypto-silver text-sm space-y-1">
                  <li>1. Copy the address above</li>
                  <li>2. Send exactly {amount} {paymentType} to this address</li>
                  <li>3. Click "I've sent the payment" below</li>
                  <li>4. Wait for blockchain confirmation</li>
                </ol>
              </div>

              <Button 
                onClick={handlePaymentConfirmation}
                className="w-full bg-gradient-to-r from-crypto-warning to-yellow-400 text-crypto-black font-semibold hover:shadow-lg"
              >
                I've Sent the Payment
              </Button>
            </>
          )}

          {paymentStatus === 'confirming' && (
            <div className="text-center py-8">
              <Clock className="w-12 h-12 text-crypto-warning mx-auto mb-4 animate-spin" />
              <h3 className="text-lg font-semibold text-white mb-2">Confirming Payment</h3>
              <p className="text-crypto-silver text-sm">
                Waiting for blockchain confirmation...
              </p>
              <div className="mt-4 text-xs text-crypto-silver">
                This may take 1-3 minutes
              </div>
            </div>
          )}

          {paymentStatus === 'confirmed' && (
            <div className="text-center py-8">
              <CheckCircle className="w-12 h-12 text-crypto-success mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-crypto-success mb-2">Payment Confirmed!</h3>
              <p className="text-crypto-silver text-sm">
                Premium whale watching access has been activated
              </p>
            </div>
          )}

          {/* Footer */}
          <div className="flex justify-between items-center pt-4 border-t border-white/[0.06]">
            <div className="text-xs text-crypto-silver">
              30-day access • Real-time alerts
            </div>
            <Button 
              variant="ghost" 
              onClick={onClose}
              className="text-crypto-silver hover:text-white"
            >
              {paymentStatus === 'confirmed' ? 'Done' : 'Cancel'}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}