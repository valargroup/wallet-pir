
/opt/transparent-publisher-build/shoup-choice-20260911/artifacts/worker-integration-tests:     file format elf64-x86-64


Disassembly of section .text:

0000000000650390 <valar_spiral_rs::ntt::avx2::ntt_forward>:
  650390:	push   %rbp
  650391:	push   %r15
  650393:	push   %r14
  650395:	push   %r13
  650397:	push   %r12
  650399:	push   %rbx
  65039a:	sub    $0xd8,%rsp
  6503a1:	mov    %rsi,0x8(%rsp)
  6503a6:	mov    0x40(%rdi),%r10
  6503aa:	cmp    $0x1,%r10
  6503ae:	jne    6506a3 <valar_spiral_rs::ntt::avx2::ntt_forward+0x313>
  6503b4:	mov    0x38(%rdi),%r8
  6503b8:	mov    %r8d,%r10d
  6503bb:	and    $0x3f,%r10d
  6503bf:	mov    $0x1,%eax
  6503c4:	shlx   %r10,%rax,%r9
  6503c9:	mov    0x8(%rdi),%rcx
  6503cd:	mov    0x10(%rdi),%rsi
  6503d1:	xor    %eax,%eax
  6503d3:	mov    %r8,0x30(%rsp)
  6503d8:	test   %r8,%r8
  6503db:	setne  %r8b
  6503df:	je     650d8c <valar_spiral_rs::ntt::avx2::ntt_forward+0x9fc>
  6503e5:	cmp    %rdx,%r9
  6503e8:	ja     650fd2 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc42>
  6503ee:	test   %rsi,%rsi
  6503f1:	je     65109b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0b>
  6503f7:	mov    0x10(%rcx),%rdx
  6503fb:	test   %rdx,%rdx
  6503fe:	je     651041 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcb1>
  650404:	mov    %r10,0x68(%rsp)
  650409:	cmp    $0x1,%rdx
  65040d:	je     651054 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcc4>
  650413:	mov    %r8b,%al
  650416:	mov    0x8(%rcx),%rcx
  65041a:	mov    0x8(%rcx),%rdx
  65041e:	mov    %rdx,0x48(%rsp)
  650423:	mov    0x10(%rcx),%rdx
  650427:	mov    %rdx,0x20(%rsp)
  65042c:	mov    0x20(%rcx),%rdx
  650430:	mov    %rdx,0x40(%rsp)
  650435:	mov    0x28(%rcx),%rcx
  650439:	mov    %rcx,0x18(%rsp)
  65043e:	mov    0xa8(%rdi),%rcx
  650445:	mov    %rcx,0x28(%rsp)
  65044a:	add    %rcx,%rcx
  65044d:	mov    %rcx,(%rsp)
  650451:	xor    %ecx,%ecx
  650453:	mov    $0x1,%edx
  650458:	mov    %r9,0x70(%rsp)
  65045d:	jmp    65047f <valar_spiral_rs::ntt::avx2::ntt_forward+0xef>
  65045f:	nop
  650460:	mov    0x38(%rsp),%rsi
  650465:	lea    0x1(%rsi),%rax
  650469:	mov    %rax,%rdx
  65046c:	mov    %rsi,%rcx
  65046f:	cmp    0x30(%rsp),%rsi
  650474:	mov    0x70(%rsp),%r9
  650479:	je     650d30 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9a0>
  65047f:	shrx   %rdx,%r9,%rbx
  650484:	mov    %rbx,%rsi
  650487:	add    %rbx,%rsi
  65048a:	mov    0x8(%rsp),%r15
  65048f:	je     650f9b <valar_spiral_rs::ntt::avx2::ntt_forward+0xc0b>
  650495:	mov    %rax,0x38(%rsp)
  65049a:	mov    $0x1,%eax
  65049f:	shlx   %rcx,%rax,%rdx
  6504a4:	mov    %rsi,%r8
  6504a7:	neg    %r8
  6504aa:	and    %r9,%r8
  6504ad:	test   %rbx,%rbx
  6504b0:	je     650660 <valar_spiral_rs::ntt::avx2::ntt_forward+0x2d0>
  6504b6:	mov    %rbx,%rax
  6504b9:	shl    $0x4,%rax
  6504bd:	mov    %rax,0x50(%rsp)
  6504c2:	lea    0x0(,%rbx,8),%rax
  6504ca:	mov    %rax,0x58(%rsp)
  6504cf:	mov    $0x1,%eax
  6504d4:	mov    %r15,0x10(%rsp)
  6504d9:	xor    %edi,%edi
  6504db:	mov    %rdx,0x78(%rsp)
  6504e0:	mov    %rsi,0x90(%rsp)
  6504e8:	nopl   0x0(%rax,%rax,1)
  6504f0:	add    %rdx,%rdi
  6504f3:	cmp    0x20(%rsp),%rdi
  6504f8:	jae    650ffe <valar_spiral_rs::ntt::avx2::ntt_forward+0xc6e>
  6504fe:	cmp    0x18(%rsp),%rdi
  650503:	jae    65101e <valar_spiral_rs::ntt::avx2::ntt_forward+0xc8e>
  650509:	sub    %rsi,%r8
  65050c:	jb     650fe3 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc53>
  650512:	mov    %rax,0x80(%rsp)
  65051a:	mov    %r8,0x88(%rsp)
  650522:	mov    0x48(%rsp),%rax
  650527:	mov    (%rax,%rdi,8),%rax
  65052b:	mov    %rax,0xa0(%rsp)
  650533:	mov    0x40(%rsp),%rax
  650538:	mov    (%rax,%rdi,8),%rax
  65053c:	mov    %rax,0x98(%rsp)
  650544:	mov    0x58(%rsp),%rax
  650549:	mov    0x10(%rsp),%rcx
  65054e:	add    %rcx,%rax
  650551:	mov    %rax,0x60(%rsp)
  650556:	xor    %ebp,%ebp
  650558:	nopl   0x0(%rax,%rax,1)
  650560:	cmp    %rbp,%rsi
  650563:	je     650f8c <valar_spiral_rs::ntt::avx2::ntt_forward+0xbfc>
  650569:	lea    (%rbx,%rbp,1),%rdi
  65056d:	cmp    %rsi,%rdi
  650570:	jae    650f80 <valar_spiral_rs::ntt::avx2::ntt_forward+0xbf0>
  650576:	mov    0x10(%rsp),%rax
  65057b:	mov    (%rax,%rbp,8),%r12
  65057f:	mov    0x60(%rsp),%rax
  650584:	mov    (%rax,%rbp,8),%rdx
  650588:	mov    (%rsp),%r15
  65058c:	cmp    %r15,%r12
  65058f:	mov    $0x0,%ecx
  650594:	cmovae %r15,%rcx
  650598:	mov    0x98(%rsp),%rax
  6505a0:	mulx   %rax,%rax,%rax
  6505a5:	mulx   0xa0(%rsp),%r14,%rsi
  6505af:	sub    %rcx,%r12
  6505b2:	mov    %rax,%rdx
  6505b5:	mov    0x28(%rsp),%rdi
  6505ba:	mulx   %rdi,%rcx,%rax
  6505bf:	sub    %rcx,%r14
  6505c2:	sbb    %rax,%rsi
  6505c5:	mov    %r14,%r13
  6505c8:	sub    %rdi,%r13
  6505cb:	sbb    $0x0,%rsi
  6505cf:	setb   %al
  6505d2:	movzbl %al,%edi
  6505d5:	call   64f360 <subtle::black_box>
  6505da:	mov    0x90(%rsp),%rsi
  6505e2:	movzbl %al,%eax
  6505e5:	mov    %rax,%rcx
  6505e8:	neg    %rcx
  6505eb:	dec    %rax
  6505ee:	and    %r13,%rax
  6505f1:	and    %r14,%rcx
  6505f4:	or     %rax,%rcx
  6505f7:	lea    (%r12,%rcx,1),%rax
  6505fb:	mov    0x10(%rsp),%rdx
  650600:	mov    %rax,(%rdx,%rbp,8)
  650604:	add    %r15,%r12
  650607:	sub    %rcx,%r12
  65060a:	mov    0x60(%rsp),%rax
  65060f:	mov    %r12,(%rax,%rbp,8)
  650613:	inc    %rbp
  650616:	cmp    %rbp,%rbx
  650619:	jne    650560 <valar_spiral_rs::ntt::avx2::ntt_forward+0x1d0>
  65061f:	mov    0x78(%rsp),%rdx
  650624:	mov    0x80(%rsp),%rdi
  65062c:	cmp    %rdx,%rdi
  65062f:	mov    %rdi,%rax
  650632:	adc    $0x0,%rax
  650636:	mov    0x10(%rsp),%rcx
  65063b:	add    0x50(%rsp),%rcx
  650640:	mov    %rcx,0x10(%rsp)
  650645:	cmp    %rdx,%rdi
  650648:	mov    0x88(%rsp),%r8
  650650:	jb     6504f0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x160>
  650656:	jmp    650460 <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0>
  65065b:	nopl   0x0(%rax,%rax,1)
  650660:	add    %rsi,%r8
  650663:	xor    %eax,%eax
  650665:	data16 cs nopw 0x0(%rax,%rax,1)
  650670:	lea    (%rdx,%rax,1),%rcx
  650674:	cmp    0x20(%rsp),%rcx
  650679:	jae    650fef <valar_spiral_rs::ntt::avx2::ntt_forward+0xc5f>
  65067f:	cmp    0x18(%rsp),%rcx
  650684:	jae    65100f <valar_spiral_rs::ntt::avx2::ntt_forward+0xc7f>
  65068a:	sub    %rsi,%r8
  65068d:	cmp    %rsi,%r8
  650690:	jb     650fe3 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc53>
  650696:	inc    %rax
  650699:	cmp    %rdx,%rax
  65069c:	jb     650670 <valar_spiral_rs::ntt::avx2::ntt_forward+0x2e0>
  65069e:	jmp    650460 <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0>
  6506a3:	test   %r10,%r10
  6506a6:	mov    0x8(%rsp),%r8
  6506ab:	je     650f6b <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  6506b1:	mov    0x38(%rdi),%r11
  6506b5:	mov    %r11d,%ebx
  6506b8:	and    $0x3f,%ebx
  6506bb:	mov    $0x1,%eax
  6506c0:	shlx   %rbx,%rax,%rsi
  6506c5:	mov    0x8(%rdi),%r15
  6506c9:	mov    0x10(%rdi),%rax
  6506cd:	mov    %rax,0x58(%rsp)
  6506d2:	mov    %rsi,%rax
  6506d5:	shr    $0x2,%rax
  6506d9:	cmp    $0x2,%ebx
  6506dc:	adc    $0x0,%rax
  6506e0:	mov    %rax,0x30(%rsp)
  6506e5:	mov    %rsi,%r12
  6506e8:	and    $0xfffffffffffffff0,%r12
  6506ec:	mov    %esi,%eax
  6506ee:	and    $0xc,%eax
  6506f1:	mov    %rax,0x20(%rsp)
  6506f6:	lea    0x60(%r8),%rax
  6506fa:	mov    %rax,0x68(%rsp)
  6506ff:	xor    %ebp,%ebp
  650701:	vbroadcasti128 -0x5d736a(%rip),%ymm0        # 793a0 <GCC_except_table6769+0x1150>
  65070a:	vpbroadcastq -0x5d42f3(%rip),%ymm1        # 7c420 <GCC_except_table6769+0x41d0>
  650713:	mov    $0x1,%eax
  650718:	xor    %ecx,%ecx
  65071a:	mov    %rdi,0x50(%rsp)
  65071f:	mov    %r10,0x48(%rsp)
  650724:	mov    %r11,0x90(%rsp)
  65072c:	mov    %rbx,0x40(%rsp)
  650731:	mov    %rsi,0x88(%rsp)
  650739:	mov    %r15,0x38(%rsp)
  65073e:	mov    %r12,0x70(%rsp)
  650743:	jmp    650768 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3d8>
  650745:	data16 cs nopw 0x0(%rax,%rax,1)
  650750:	mov    0x18(%rsp),%rcx
  650755:	cmp    %r10,%rcx
  650758:	mov    %rcx,%rax
  65075b:	adc    $0x0,%rax
  65075f:	cmp    %r10,%rcx
  650762:	jae    650f6b <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  650768:	cmp    0x58(%rsp),%rcx
  65076d:	jae    65106d <valar_spiral_rs::ntt::avx2::ntt_forward+0xcdd>
  650773:	mov    %rax,%r9
  650776:	lea    (%rcx,%rcx,2),%rax
  65077a:	mov    0x10(%r15,%rax,8),%rdx
  65077f:	test   %rdx,%rdx
  650782:	je     651041 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcb1>
  650788:	cmp    $0x1,%rdx
  65078c:	je     651054 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcc4>
  650792:	mov    %r9,0x18(%rsp)
  650797:	cmp    $0x3,%rcx
  65079b:	ja     651084 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcf4>
  6507a1:	shlx   %rbx,%rcx,%r14
  6507a6:	lea    (%r8,%r14,8),%rdx
  6507aa:	mov    %rdx,(%rsp)
  6507ae:	mov    0xa8(%rdi,%rcx,8),%rdx
  6507b6:	lea    (%rdx,%rdx,1),%ecx
  6507b9:	mov    %edx,%r9d
  6507bc:	test   %r11,%r11
  6507bf:	je     650b42 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7b2>
  6507c5:	lea    (%r15,%rax,8),%rax
  6507c9:	mov    0x8(%rax),%rax
  6507cd:	mov    0x8(%rax),%r15
  6507d1:	mov    0x20(%rax),%r13
  6507d5:	vmovq  %rcx,%xmm2
  6507da:	vpbroadcastq %xmm2,%ymm2
  6507df:	vmovq  %r9,%xmm3
  6507e4:	vpbroadcastq %xmm3,%ymm3
  6507e9:	vmovd  %ecx,%xmm4
  6507ed:	vpbroadcastd %xmm4,%xmm4
  6507f2:	lea    (%r8,%r14,8),%rax
  6507f6:	mov    %rax,0x80(%rsp)
  6507fe:	shl    $0x3,%r14
  650802:	mov    %r14,0x78(%rsp)
  650807:	mov    $0x1,%eax
  65080c:	xor    %edx,%edx
  65080e:	mov    %r15,0x60(%rsp)
  650813:	mov    %r13,0xa0(%rsp)
  65081b:	jmp    650842 <valar_spiral_rs::ntt::avx2::ntt_forward+0x4b2>
  65081d:	nopl   (%rax)
  650820:	mov    0x28(%rsp),%rdx
  650825:	lea    0x1(%rdx),%rax
  650829:	mov    0x90(%rsp),%r11
  650831:	cmp    %r11,%rdx
  650834:	mov    0x88(%rsp),%rsi
  65083c:	je     650b10 <valar_spiral_rs::ntt::avx2::ntt_forward+0x780>
  650842:	mov    $0x1,%edi
  650847:	shlx   %rdx,%rdi,%rbx
  65084c:	mov    %rax,%rdx
  65084f:	cmp    $0xb,%r11
  650853:	setb   %al
  650856:	mov    %rdx,0x28(%rsp)
  65085b:	shrx   %rdx,%rsi,%r12
  650860:	cmp    $0x4,%r12
  650864:	setb   %dl
  650867:	or     %al,%dl
  650869:	mov    %r12,%rax
  65086c:	shr    $0x2,%rax
  650870:	mov    %r12d,%esi
  650873:	and    $0x3,%esi
  650876:	cmp    $0x1,%rsi
  65087a:	sbb    $0xffffffffffffffff,%rax
  65087e:	test   %dl,%dl
  650880:	je     650a30 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6a0>
  650886:	test   %r12,%r12
  650889:	je     650820 <valar_spiral_rs::ntt::avx2::ntt_forward+0x490>
  65088b:	mov    %r12,%r8
  65088e:	and    $0xfffffffffffffffc,%r8
  650892:	mov    %r12,%rax
  650895:	shl    $0x4,%rax
  650899:	mov    %rax,0x10(%rsp)
  65089e:	lea    0x0(,%r12,8),%rax
  6508a6:	mov    %rax,0x98(%rsp)
  6508ae:	mov    0x80(%rsp),%rax
  6508b6:	lea    (%rax,%r12,8),%rax
  6508ba:	mov    (%rsp),%r14
  6508be:	xor    %edx,%edx
  6508c0:	jmp    6508fb <valar_spiral_rs::ntt::avx2::ntt_forward+0x56b>
  6508c2:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  6508d0:	cmp    %rbx,%rdx
  6508d3:	mov    %rdx,%rdi
  6508d6:	adc    $0x0,%rdi
  6508da:	mov    0x10(%rsp),%rsi
  6508df:	add    %rsi,%r14
  6508e2:	add    %rsi,%rax
  6508e5:	cmp    %rbx,%rdx
  6508e8:	mov    0x60(%rsp),%r15
  6508ed:	mov    0xa0(%rsp),%r13
  6508f5:	jae    650820 <valar_spiral_rs::ntt::avx2::ntt_forward+0x490>
  6508fb:	add    %rbx,%rdx
  6508fe:	mov    (%r15,%rdx,8),%rsi
  650902:	mov    0x0(%r13,%rdx,8),%r15
  650907:	mov    %rdi,%rdx
  65090a:	cmp    $0x4,%r12
  65090e:	jae    650920 <valar_spiral_rs::ntt::avx2::ntt_forward+0x590>
  650910:	xor    %r13d,%r13d
  650913:	jmp    6509e0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x650>
  650918:	nopl   0x0(%rax,%rax,1)
  650920:	vmovq  %rsi,%xmm5
  650925:	vpbroadcastq %xmm5,%ymm5
  65092a:	vmovq  %r15,%xmm6
  65092f:	vpbroadcastq %xmm6,%ymm6
  650934:	mov    0x98(%rsp),%rdi
  65093c:	lea    (%r14,%rdi,1),%r13
  650940:	vpsrlq $0x20,%ymm6,%ymm7
  650945:	vpsrlq $0x20,%ymm5,%ymm8
  65094a:	xor    %edi,%edi
  65094c:	nopl   0x0(%rax)
  650950:	vpermd (%r14,%rdi,8),%ymm0,%ymm9
  650956:	vmovdqu 0x0(%r13,%rdi,8),%ymm10
  65095d:	vpminud %xmm9,%xmm4,%xmm11
  650962:	vpcmpeqd %xmm4,%xmm11,%xmm11
  650966:	vpand  %xmm4,%xmm11,%xmm11
  65096a:	vpsubd %xmm11,%xmm9,%xmm9
  65096f:	vpmuludq %ymm6,%ymm10,%ymm11
  650973:	vpmuludq %ymm7,%ymm10,%ymm12
  650977:	vpsllq $0x20,%ymm12,%ymm12
  65097d:	vpaddq %ymm12,%ymm11,%ymm11
  650982:	vpsrlq $0x20,%ymm11,%ymm11
  650988:	vpmuludq %ymm5,%ymm10,%ymm12
  65098c:	vpmuludq %ymm8,%ymm10,%ymm10
  650991:	vpsllq $0x20,%ymm10,%ymm10
  650997:	vpaddq %ymm10,%ymm12,%ymm10
  65099c:	vpmuludq %ymm3,%ymm11,%ymm11
  6509a0:	vpsubq %ymm11,%ymm10,%ymm10
  6509a5:	vpmovzxdq %xmm9,%ymm9
  6509aa:	vpaddq %ymm9,%ymm10,%ymm11
  6509af:	vmovdqu %ymm11,(%r14,%rdi,8)
  6509b5:	vpaddq %ymm2,%ymm9,%ymm9
  6509b9:	vpsubq %ymm10,%ymm9,%ymm9
  6509be:	vmovdqu %ymm9,0x0(%r13,%rdi,8)
  6509c5:	add    $0x4,%rdi
  6509c9:	cmp    %rdi,%r8
  6509cc:	jne    650950 <valar_spiral_rs::ntt::avx2::ntt_forward+0x5c0>
  6509ce:	mov    %r8,%r13
  6509d1:	cmp    %r8,%r12
  6509d4:	je     6508d0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x540>
  6509da:	nopw   0x0(%rax,%rax,1)
  6509e0:	mov    (%r14,%r13,8),%edi
  6509e4:	mov    (%rax,%r13,8),%r11d
  6509e8:	cmp    %edi,%ecx
  6509ea:	mov    %ecx,%r10d
  6509ed:	cmova  %ebp,%r10d
  6509f1:	sub    %r10d,%edi
  6509f4:	mov    %r11,%r10
  6509f7:	imul   %r15,%r10
  6509fb:	shr    $0x20,%r10
  6509ff:	imul   %rsi,%r11
  650a03:	imul   %r9,%r10
  650a07:	sub    %r10,%r11
  650a0a:	lea    (%r11,%rdi,1),%r10
  650a0e:	mov    %r10,(%r14,%r13,8)
  650a12:	add    %rcx,%rdi
  650a15:	sub    %r11,%rdi
  650a18:	mov    %rdi,(%rax,%r13,8)
  650a1c:	inc    %r13
  650a1f:	cmp    %r13,%r12
  650a22:	jne    6509e0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x650>
  650a24:	jmp    6508d0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x540>
  650a29:	nopl   0x0(%rax)
  650a30:	mov    %r12,%rdx
  650a33:	shl    $0x4,%rdx
  650a37:	mov    (%rsp),%rsi
  650a3b:	xor    %r8d,%r8d
  650a3e:	xchg   %ax,%ax
  650a40:	add    %rbx,%r8
  650a43:	vpmovzxdq 0x0(%r13,%r8,8),%xmm5
  650a4a:	vpmovzxdq (%r15,%r8,8),%xmm6
  650a50:	mov    %rdi,%r8
  650a53:	vpbroadcastq %xmm5,%ymm5
  650a58:	vpbroadcastq %xmm6,%ymm6
  650a5d:	mov    %rsi,%r10
  650a60:	mov    %rax,%r14
  650a63:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  650a70:	vmovdqa (%r10),%ymm7
  650a75:	vmovdqa (%r10,%r12,8),%ymm8
  650a7b:	vpcmpgtq %ymm2,%ymm7,%ymm9
  650a80:	vpand  %ymm2,%ymm9,%ymm9
  650a84:	vpsubq %ymm9,%ymm7,%ymm7
  650a89:	vpmuludq %ymm5,%ymm8,%ymm9
  650a8d:	vpsrlq $0x20,%ymm5,%ymm10
  650a92:	vpmuludq %ymm10,%ymm8,%ymm10
  650a97:	vpsllq $0x20,%ymm10,%ymm10
  650a9d:	vpaddq %ymm10,%ymm9,%ymm9
  650aa2:	vpsrlq $0x20,%ymm9,%ymm9
  650aa8:	vpmuludq %ymm6,%ymm8,%ymm10
  650aac:	vpsrlq $0x20,%ymm6,%ymm11
  650ab1:	vpmuludq %ymm11,%ymm8,%ymm8
  650ab6:	vpsllq $0x20,%ymm8,%ymm8
  650abc:	vpaddq %ymm8,%ymm10,%ymm8
  650ac1:	vpmuludq %ymm3,%ymm9,%ymm9
  650ac5:	vpsubq %ymm9,%ymm8,%ymm8
  650aca:	vpaddq %ymm7,%ymm8,%ymm9
  650ace:	vpaddq %ymm2,%ymm7,%ymm7
  650ad2:	vpsubq %ymm8,%ymm7,%ymm7
  650ad7:	vmovdqa %ymm9,(%r10)
  650adc:	vmovdqa %ymm7,(%r10,%r12,8)
  650ae2:	add    $0x20,%r10
  650ae6:	dec    %r14
  650ae9:	jne    650a70 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6e0>
  650aeb:	cmp    %rbx,%r8
  650aee:	mov    %r8,%rdi
  650af1:	adc    $0x0,%rdi
  650af5:	add    %rdx,%rsi
  650af8:	cmp    %rbx,%r8
  650afb:	jb     650a40 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6b0>
  650b01:	jmp    650820 <valar_spiral_rs::ntt::avx2::ntt_forward+0x490>
  650b06:	cs nopw 0x0(%rax,%rax,1)
  650b10:	cmp    $0xa,%r11
  650b14:	ja     650b90 <valar_spiral_rs::ntt::avx2::ntt_forward+0x800>
  650b16:	mov    0x40(%rsp),%rbx
  650b1b:	cmp    $0x1,%ebx
  650b1e:	mov    0x8(%rsp),%r8
  650b23:	mov    0x50(%rsp),%rdi
  650b28:	mov    0x48(%rsp),%r10
  650b2d:	mov    0x38(%rsp),%r15
  650b32:	mov    0x70(%rsp),%r12
  650b37:	mov    0x78(%rsp),%rdx
  650b3c:	ja     650bfb <valar_spiral_rs::ntt::avx2::ntt_forward+0x86b>
  650b42:	xor    %eax,%eax
  650b44:	mov    (%rsp),%rdx
  650b48:	jmp    650b6a <valar_spiral_rs::ntt::avx2::ntt_forward+0x7da>
  650b4a:	nopw   0x0(%rax,%rax,1)
  650b50:	sub    %r14,%rdx
  650b53:	mov    (%rsp),%r14
  650b57:	mov    %rdx,(%r14,%rax,8)
  650b5b:	mov    %r14,%rdx
  650b5e:	inc    %rax
  650b61:	cmp    %rax,%rsi
  650b64:	je     650750 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  650b6a:	mov    (%rdx,%rax,8),%rdx
  650b6e:	mov    $0x0,%r14d
  650b74:	cmp    %rcx,%rdx
  650b77:	jb     650b7c <valar_spiral_rs::ntt::avx2::ntt_forward+0x7ec>
  650b79:	mov    %rcx,%r14
  650b7c:	sub    %r14,%rdx
  650b7f:	mov    $0x0,%r14d
  650b85:	cmp    %r9,%rdx
  650b88:	jb     650b50 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7c0>
  650b8a:	mov    %r9,%r14
  650b8d:	jmp    650b50 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7c0>
  650b8f:	nop
  650b90:	cmpq   $0x0,0x30(%rsp)
  650b96:	mov    0x8(%rsp),%r8
  650b9b:	mov    0x50(%rsp),%rdi
  650ba0:	mov    0x48(%rsp),%r10
  650ba5:	mov    0x40(%rsp),%rbx
  650baa:	mov    0x38(%rsp),%r15
  650baf:	mov    (%rsp),%rdx
  650bb3:	je     650750 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  650bb9:	xor    %ecx,%ecx
  650bbb:	mov    0x30(%rsp),%rax
  650bc0:	cmp    %rsi,%rcx
  650bc3:	jae    65102f <valar_spiral_rs::ntt::avx2::ntt_forward+0xc9f>
  650bc9:	vmovdqa (%rdx,%rcx,8),%ymm4
  650bce:	vpcmpgtq %ymm2,%ymm4,%ymm5
  650bd3:	vpand  %ymm2,%ymm5,%ymm5
  650bd7:	vpsubq %ymm5,%ymm4,%ymm4
  650bdb:	vpcmpgtq %ymm3,%ymm4,%ymm5
  650be0:	vpand  %ymm3,%ymm5,%ymm5
  650be4:	vpsubq %ymm5,%ymm4,%ymm4
  650be8:	vmovdqa %ymm4,(%rdx,%rcx,8)
  650bed:	add    $0x4,%rcx
  650bf1:	dec    %rax
  650bf4:	jne    650bc0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x830>
  650bf6:	jmp    650750 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  650bfb:	vpor   %ymm1,%ymm2,%ymm4
  650bff:	vpor   %ymm1,%ymm3,%ymm5
  650c03:	cmp    $0x4,%ebx
  650c06:	jae    650c4c <valar_spiral_rs::ntt::avx2::ntt_forward+0x8bc>
  650c08:	xor    %eax,%eax
  650c0a:	mov    (%rsp),%rcx
  650c0e:	xchg   %ax,%ax
  650c10:	vmovdqu (%rcx,%rax,8),%ymm6
  650c15:	vpxor  %ymm1,%ymm6,%ymm7
  650c19:	vpcmpgtq %ymm7,%ymm4,%ymm7
  650c1e:	vpandn %ymm2,%ymm7,%ymm7
  650c22:	vpsubq %ymm7,%ymm6,%ymm6
  650c26:	vpxor  %ymm1,%ymm6,%ymm7
  650c2a:	vpcmpgtq %ymm7,%ymm5,%ymm7
  650c2f:	vpandn %ymm3,%ymm7,%ymm7
  650c33:	vpsubq %ymm7,%ymm6,%ymm6
  650c37:	vmovdqu %ymm6,(%rcx,%rax,8)
  650c3c:	add    $0x4,%rax
  650c40:	cmp    %rax,0x20(%rsp)
  650c45:	jne    650c10 <valar_spiral_rs::ntt::avx2::ntt_forward+0x880>
  650c47:	jmp    650750 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  650c4c:	add    0x68(%rsp),%rdx
  650c51:	xor    %eax,%eax
  650c53:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  650c60:	vmovdqu -0x60(%rdx,%rax,8),%ymm6
  650c66:	vmovdqu -0x40(%rdx,%rax,8),%ymm7
  650c6c:	vmovdqu -0x20(%rdx,%rax,8),%ymm8
  650c72:	vmovdqu (%rdx,%rax,8),%ymm9
  650c77:	vpxor  %ymm1,%ymm6,%ymm10
  650c7b:	vpcmpgtq %ymm10,%ymm4,%ymm10
  650c80:	vpandn %ymm2,%ymm10,%ymm10
  650c84:	vpxor  %ymm1,%ymm7,%ymm11
  650c88:	vpcmpgtq %ymm11,%ymm4,%ymm11
  650c8d:	vpandn %ymm2,%ymm11,%ymm11
  650c91:	vpxor  %ymm1,%ymm8,%ymm12
  650c95:	vpcmpgtq %ymm12,%ymm4,%ymm12
  650c9a:	vpandn %ymm2,%ymm12,%ymm12
  650c9e:	vpxor  %ymm1,%ymm9,%ymm13
  650ca2:	vpcmpgtq %ymm13,%ymm4,%ymm13
  650ca7:	vpandn %ymm2,%ymm13,%ymm13
  650cab:	vpsubq %ymm10,%ymm6,%ymm6
  650cb0:	vpsubq %ymm11,%ymm7,%ymm7
  650cb5:	vpsubq %ymm12,%ymm8,%ymm8
  650cba:	vpsubq %ymm13,%ymm9,%ymm9
  650cbf:	vpxor  %ymm1,%ymm6,%ymm10
  650cc3:	vpcmpgtq %ymm10,%ymm5,%ymm10
  650cc8:	vpandn %ymm3,%ymm10,%ymm10
  650ccc:	vpxor  %ymm1,%ymm7,%ymm11
  650cd0:	vpcmpgtq %ymm11,%ymm5,%ymm11
  650cd5:	vpandn %ymm3,%ymm11,%ymm11
  650cd9:	vpxor  %ymm1,%ymm8,%ymm12
  650cdd:	vpcmpgtq %ymm12,%ymm5,%ymm12
  650ce2:	vpandn %ymm3,%ymm12,%ymm12
  650ce6:	vpxor  %ymm1,%ymm9,%ymm13
  650cea:	vpcmpgtq %ymm13,%ymm5,%ymm13
  650cef:	vpandn %ymm3,%ymm13,%ymm13
  650cf3:	vpsubq %ymm10,%ymm6,%ymm6
  650cf8:	vpsubq %ymm11,%ymm7,%ymm7
  650cfd:	vpsubq %ymm12,%ymm8,%ymm8
  650d02:	vpsubq %ymm13,%ymm9,%ymm9
  650d07:	vmovdqu %ymm6,-0x60(%rdx,%rax,8)
  650d0d:	vmovdqu %ymm7,-0x40(%rdx,%rax,8)
  650d13:	vmovdqu %ymm8,-0x20(%rdx,%rax,8)
  650d19:	vmovdqu %ymm9,(%rdx,%rax,8)
  650d1e:	add    $0x10,%rax
  650d22:	cmp    %rax,%r12
  650d25:	jne    650c60 <valar_spiral_rs::ntt::avx2::ntt_forward+0x8d0>
  650d2b:	jmp    650750 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  650d30:	mov    0x68(%rsp),%rax
  650d35:	cmp    $0x1,%eax
  650d38:	mov    0x8(%rsp),%rsi
  650d3d:	ja     650e0b <valar_spiral_rs::ntt::avx2::ntt_forward+0xa7b>
  650d43:	xor    %eax,%eax
  650d45:	jmp    650d63 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9d3>
  650d47:	nopw   0x0(%rax,%rax,1)
  650d50:	sub    %rdx,%rcx
  650d53:	mov    %rcx,(%rsi,%rax,8)
  650d57:	inc    %rax
  650d5a:	cmp    %rax,%r9
  650d5d:	je     650f6b <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  650d63:	mov    (%rsi,%rax,8),%rcx
  650d67:	mov    $0x0,%edx
  650d6c:	cmp    (%rsp),%rcx
  650d70:	jb     650d76 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9e6>
  650d72:	mov    (%rsp),%rdx
  650d76:	sub    %rdx,%rcx
  650d79:	mov    $0x0,%edx
  650d7e:	cmp    0x28(%rsp),%rcx
  650d83:	jb     650d50 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9c0>
  650d85:	mov    0x28(%rsp),%rdx
  650d8a:	jmp    650d50 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9c0>
  650d8c:	cmp    %rdx,%r9
  650d8f:	ja     650fd2 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc42>
  650d95:	test   %rsi,%rsi
  650d98:	je     65109b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0b>
  650d9e:	mov    0x10(%rcx),%rax
  650da2:	test   %rax,%rax
  650da5:	je     651041 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcb1>
  650dab:	cmp    $0x1,%rax
  650daf:	je     651054 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcc4>
  650db5:	mov    0xa8(%rdi),%rax
  650dbc:	lea    (%rax,%rax,1),%rcx
  650dc0:	xor    %edx,%edx
  650dc2:	mov    0x8(%rsp),%rdi
  650dc7:	jmp    650de8 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa58>
  650dc9:	nopl   0x0(%rax)
  650dd0:	sub    %rdi,%rsi
  650dd3:	mov    0x8(%rsp),%rdi
  650dd8:	mov    %rsi,(%rdi,%rdx,8)
  650ddc:	inc    %rdx
  650ddf:	cmp    %rdx,%r9
  650de2:	je     650f6b <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  650de8:	mov    (%rdi,%rdx,8),%rsi
  650dec:	mov    $0x0,%edi
  650df1:	cmp    %rcx,%rsi
  650df4:	jb     650df9 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa69>
  650df6:	mov    %rcx,%rdi
  650df9:	sub    %rdi,%rsi
  650dfc:	mov    $0x0,%edi
  650e01:	cmp    %rax,%rsi
  650e04:	jb     650dd0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa40>
  650e06:	mov    %rax,%rdi
  650e09:	jmp    650dd0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa40>
  650e0b:	vmovq  (%rsp),%xmm0
  650e10:	vpbroadcastq %xmm0,%ymm0
  650e15:	vmovq  0x28(%rsp),%xmm1
  650e1b:	vpbroadcastq %xmm1,%ymm1
  650e20:	cmp    $0x4,%eax
  650e23:	jae    650e7a <valar_spiral_rs::ntt::avx2::ntt_forward+0xaea>
  650e25:	and    $0xc,%r9d
  650e29:	xor    %eax,%eax
  650e2b:	vpbroadcastq -0x5d4a14(%rip),%ymm2        # 7c420 <GCC_except_table6769+0x41d0>
  650e34:	vpxor  %ymm2,%ymm0,%ymm3
  650e38:	vpxor  %ymm2,%ymm1,%ymm4
  650e3c:	nopl   0x0(%rax)
  650e40:	vmovdqu (%rsi,%rax,8),%ymm5
  650e45:	vpxor  %ymm2,%ymm5,%ymm6
  650e49:	vpcmpgtq %ymm6,%ymm3,%ymm6
  650e4e:	vpandn %ymm0,%ymm6,%ymm6
  650e52:	vpsubq %ymm6,%ymm5,%ymm5
  650e56:	vpxor  %ymm2,%ymm5,%ymm6
  650e5a:	vpcmpgtq %ymm6,%ymm4,%ymm6
  650e5f:	vpandn %ymm1,%ymm6,%ymm6
  650e63:	vpsubq %ymm6,%ymm5,%ymm5
  650e67:	vmovdqu %ymm5,(%rsi,%rax,8)
  650e6c:	add    $0x4,%rax
  650e70:	cmp    %rax,%r9
  650e73:	jne    650e40 <valar_spiral_rs::ntt::avx2::ntt_forward+0xab0>
  650e75:	jmp    650f6b <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  650e7a:	and    $0xfffffffffffffff0,%r9
  650e7e:	xor    %eax,%eax
  650e80:	vpbroadcastq -0x5d4a69(%rip),%ymm2        # 7c420 <GCC_except_table6769+0x41d0>
  650e89:	vpxor  %ymm2,%ymm0,%ymm3
  650e8d:	vpxor  %ymm2,%ymm1,%ymm4
  650e91:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  650ea0:	vmovdqu (%rsi,%rax,8),%ymm5
  650ea5:	vmovdqu 0x20(%rsi,%rax,8),%ymm6
  650eab:	vmovdqu 0x40(%rsi,%rax,8),%ymm7
  650eb1:	vmovdqu 0x60(%rsi,%rax,8),%ymm8
  650eb7:	vpxor  %ymm2,%ymm5,%ymm9
  650ebb:	vpcmpgtq %ymm9,%ymm3,%ymm9
  650ec0:	vpandn %ymm0,%ymm9,%ymm9
  650ec4:	vpxor  %ymm2,%ymm6,%ymm10
  650ec8:	vpcmpgtq %ymm10,%ymm3,%ymm10
  650ecd:	vpandn %ymm0,%ymm10,%ymm10
  650ed1:	vpxor  %ymm2,%ymm7,%ymm11
  650ed5:	vpcmpgtq %ymm11,%ymm3,%ymm11
  650eda:	vpandn %ymm0,%ymm11,%ymm11
  650ede:	vpxor  %ymm2,%ymm8,%ymm12
  650ee2:	vpcmpgtq %ymm12,%ymm3,%ymm12
  650ee7:	vpandn %ymm0,%ymm12,%ymm12
  650eeb:	vpsubq %ymm9,%ymm5,%ymm5
  650ef0:	vpsubq %ymm10,%ymm6,%ymm6
  650ef5:	vpsubq %ymm11,%ymm7,%ymm7
  650efa:	vpsubq %ymm12,%ymm8,%ymm8
  650eff:	vpxor  %ymm2,%ymm5,%ymm9
  650f03:	vpcmpgtq %ymm9,%ymm4,%ymm9
  650f08:	vpandn %ymm1,%ymm9,%ymm9
  650f0c:	vpxor  %ymm2,%ymm6,%ymm10
  650f10:	vpcmpgtq %ymm10,%ymm4,%ymm10
  650f15:	vpandn %ymm1,%ymm10,%ymm10
  650f19:	vpxor  %ymm2,%ymm7,%ymm11
  650f1d:	vpcmpgtq %ymm11,%ymm4,%ymm11
  650f22:	vpandn %ymm1,%ymm11,%ymm11
  650f26:	vpxor  %ymm2,%ymm8,%ymm12
  650f2a:	vpcmpgtq %ymm12,%ymm4,%ymm12
  650f2f:	vpandn %ymm1,%ymm12,%ymm12
  650f33:	vpsubq %ymm9,%ymm5,%ymm5
  650f38:	vpsubq %ymm10,%ymm6,%ymm6
  650f3d:	vpsubq %ymm11,%ymm7,%ymm7
  650f42:	vpsubq %ymm12,%ymm8,%ymm8
  650f47:	vmovdqu %ymm5,(%rsi,%rax,8)
  650f4c:	vmovdqu %ymm6,0x20(%rsi,%rax,8)
  650f52:	vmovdqu %ymm7,0x40(%rsi,%rax,8)
  650f58:	vmovdqu %ymm8,0x60(%rsi,%rax,8)
  650f5e:	add    $0x10,%rax
  650f62:	cmp    %rax,%r9
  650f65:	jne    650ea0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb10>
  650f6b:	add    $0xd8,%rsp
  650f72:	pop    %rbx
  650f73:	pop    %r12
  650f75:	pop    %r13
  650f77:	pop    %r14
  650f79:	pop    %r15
  650f7b:	pop    %rbp
  650f7c:	vzeroupper
  650f7f:	ret
  650f80:	lea    0x84789(%rip),%rdx        # 6d5710 <tokio::runtime::task::waker::WAKER_VTABLE+0x2438>
  650f87:	call   28be70 <core::panicking::panic_bounds_check>
  650f8c:	lea    0x84765(%rip),%rdx        # 6d56f8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2420>
  650f93:	mov    %rbp,%rdi
  650f96:	call   28be70 <core::panicking::panic_bounds_check>
  650f9b:	lea    0x84cc6(%rip),%rax        # 6d5c68 <tokio::runtime::task::waker::WAKER_VTABLE+0x2990>
  650fa2:	mov    %rax,0xa8(%rsp)
  650faa:	vmovaps -0x5d5672(%rip),%ymm0        # 7b940 <GCC_except_table6769+0x36f0>
  650fb2:	vmovups %ymm0,0xb0(%rsp)
  650fbb:	lea    0x846d6(%rip),%rsi        # 6d5698 <tokio::runtime::task::waker::WAKER_VTABLE+0x23c0>
  650fc2:	lea    0xa8(%rsp),%rdi
  650fca:	vzeroupper
  650fcd:	call   2883e0 <core::panicking::panic_fmt>
  650fd2:	lea    0x8474f(%rip),%rcx        # 6d5728 <tokio::runtime::task::waker::WAKER_VTABLE+0x2450>
  650fd9:	xor    %edi,%edi
  650fdb:	mov    %r9,%rsi
  650fde:	call   2896c0 <core::slice::index::slice_index_fail>
  650fe3:	lea    0x846f6(%rip),%rdi        # 6d56e0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2408>
  650fea:	call   288400 <core::option::unwrap_failed>
  650fef:	mov    0x20(%rsp),%rax
  650ff4:	cmp    %rdx,%rax
  650ff7:	cmova  %rax,%rdx
  650ffb:	mov    %rdx,%rdi
  650ffe:	lea    0x846ab(%rip),%rdx        # 6d56b0 <tokio::runtime::task::waker::WAKER_VTABLE+0x23d8>
  651005:	mov    0x20(%rsp),%rsi
  65100a:	call   28be70 <core::panicking::panic_bounds_check>
  65100f:	mov    0x18(%rsp),%rax
  651014:	cmp    %rdx,%rax
  651017:	cmova  %rax,%rdx
  65101b:	mov    %rdx,%rdi
  65101e:	lea    0x846a3(%rip),%rdx        # 6d56c8 <tokio::runtime::task::waker::WAKER_VTABLE+0x23f0>
  651025:	mov    0x18(%rsp),%rsi
  65102a:	call   28be70 <core::panicking::panic_bounds_check>
  65102f:	lea    0x847ca(%rip),%rdx        # 6d5800 <tokio::runtime::task::waker::WAKER_VTABLE+0x2528>
  651036:	mov    %rcx,%rdi
  651039:	vzeroupper
  65103c:	call   28be70 <core::panicking::panic_bounds_check>
  651041:	lea    0x84cc0(%rip),%rdx        # 6d5d08 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a30>
  651048:	xor    %edi,%edi
  65104a:	xor    %esi,%esi
  65104c:	vzeroupper
  65104f:	call   28be70 <core::panicking::panic_bounds_check>
  651054:	lea    0x84cf5(%rip),%rdx        # 6d5d50 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a78>
  65105b:	mov    $0x1,%edi
  651060:	mov    $0x1,%esi
  651065:	vzeroupper
  651068:	call   28be70 <core::panicking::panic_bounds_check>
  65106d:	lea    0x84c7c(%rip),%rdx        # 6d5cf0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a18>
  651074:	mov    %rcx,%rdi
  651077:	mov    0x58(%rsp),%rsi
  65107c:	vzeroupper
  65107f:	call   28be70 <core::panicking::panic_bounds_check>
  651084:	lea    0x8475d(%rip),%rdx        # 6d57e8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2510>
  65108b:	mov    $0x4,%esi
  651090:	mov    %rcx,%rdi
  651093:	vzeroupper
  651096:	call   28be70 <core::panicking::panic_bounds_check>
  65109b:	lea    0x84c4e(%rip),%rdx        # 6d5cf0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a18>
  6510a2:	xor    %edi,%edi
  6510a4:	call   28be70 <core::panicking::panic_bounds_check>
